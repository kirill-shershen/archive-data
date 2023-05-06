import csv
import datetime
import io
import os
import zipfile

import boto3
from botocore.exceptions import ClientError

import config
from db import get_data, get_archive_date, get_oldest_date, delete_data, vacuum_db
from exceptions import NoFilesException, NoConfigException


def get_table_list() -> dict:
    """Getting information about what table and which column we need"""
    tables = {}
    table_list_file = os.path.join(config.S3_SETTINGS, "tables.csv")
    try:
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=config.S3_BUCKET, Key=table_list_file)
    except Exception:
        return {}
    lines = response["Body"].read().decode("utf-8")
    if not lines.strip():
        return {}
    for row in lines.split("\n"):
        if row:
            table, column = row.split(";")
            tables[table] = column.strip()
    config.logger.debug(tables)
    return tables


def put_data_to_s3(
    data: list, columns: list, table_name: str, date_from: datetime, date_to: datetime
) -> None:
    s3 = boto3.resource("s3")
    filename = f"events_{table_name}_{date_to.strftime('%m.%d.%Y')}_{date_from.strftime('%m.%d.%Y')}.csv"
    key = os.path.join(config.S3_ARCHIVE, filename)
    with io.StringIO() as csv_buffer:
        writer = csv.writer(csv_buffer)
        writer.writerow(columns)
        for row in data:
            writer.writerow(row)
        s3.Object(config.S3_BUCKET, key).put(Body=csv_buffer.getvalue())
    config.logger.debug(f"saved to {filename}")


def write_old_data_to_s3(
    data: list,
    columns: list,
    table_name: str,
    date_column: str,
    date_from: datetime,
    date_to: datetime,
):
    put_data_to_s3(
        data=data,
        columns=columns,
        table_name=table_name,
        date_from=date_from,
        date_to=date_to,
    )
    delete_data(table_name=table_name, column_name=date_column, date_from=date_from)


def clean_s3():
    s3 = boto3.resource("s3")
    for obj in s3.Bucket(config.S3_BUCKET).objects.filter(Prefix=config.S3_ARCHIVE):
        if obj.key.endswith("/"):
            continue
        try:
            s3.Object(config.S3_BUCKET, obj.key).delete()
        except Exception:
            pass
    config.logger.debug("s3 archive is cleared")


def archive_old_data():
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(config.S3_BUCKET)

    objs = bucket.objects.filter(Prefix=config.S3_ARCHIVE)

    buffer = io.BytesIO()

    with zipfile.ZipFile(
        buffer, mode="w", compression=zipfile.ZIP_DEFLATED
    ) as zip_archive:
        for obj in objs:
            if obj.key.endswith("/"):
                continue
            obj_data = obj.get()["Body"].read()
            file_name = obj.key.split("/")[-1]
            zip_archive.writestr(file_name, obj_data)

    buffer.seek(0)

    glacier = boto3.client("glacier")
    archive = glacier.upload_archive(
        vaultName=config.GLACIER_VAULT,
        archiveDescription="events archive from rds",
        body=buffer.getvalue(),
    )
    return archive["archiveId"]


def send_email(error: bool = False, msg: str = "") -> dict:
    config.logger.debug("sending email...")
    body = ""
    if not error:
        subject = "events archiver problem"
        body = (
            "Hello everyone,\n pleased to inform you that the operation to archive outdated data from our "
            "database to S3 Glacier has been successfully completed. "
        )
    else:
        body = (
            "Hello everyone,\n"
            "regret to inform you that the outdated data archiving operation from "
            "our database to S3 Glacier was unsuccessful.\n\n"
            f"Error message: '{msg}'"
        )
        subject = "events archiver"
    client = boto3.client("ses", region_name=config.AWS_REGION)
    message = {
        "Subject": {"Data": subject},
        "Body": {"Text": {"Data": body}},
    }

    try:
        response = client.send_email(
            Source=config.EMAIL_SENDER,
            Destination={"ToAddresses": [config.EMAIL_RECIPIENT]},
            Message=message,
        )
    except ClientError as e:
        config.logger.error(f"Error sending email: {e.response['Error']['Message']}")
        return False
    else:
        config.logger.info(f"Email sent! Message ID: {response['MessageId']}")
        return True


def raise_if_no_files_found() -> None:
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(config.S3_BUCKET)
    for obj in bucket.objects.filter(Prefix=config.S3_ARCHIVE):
        if obj.key.endswith("/"):
            continue
        return None
    raise NoFilesException(
        f"No files found in {os.path.join(config.S3_ARCHIVE)} on S3 bucket {config.S3_BUCKET}"
    )


def lambda_handler(event, context):
    os.environ["AWS_REGION"] = config.AWS_REGION
    try:
        event_date = event.get("date")
        data_from = None
        if event_date:
            try:
                data_from = datetime.strptime(event_date, "%Y-%m-%d").date()
            except Exception:
                data_from = None
        if not data_from:
            date_from = get_archive_date()
        config.logger.info(f"Operation date is {date_from}")

        tables = get_table_list()
        if not tables:
            raise NoConfigException(
                f"No config files found in {config.S3_SETTINGS} on S3 bucket {config.S3_BUCKET}"
            )

        for table, date_column in tables.items():
            config.logger.info(f"getting info from {table} and saving to s3...")
            date_to = get_oldest_date(table_name=table, column_name=date_column)
            is_date_gt_date_from = (
                isinstance(date_to, datetime.date)
                and datetime.datetime.combine(date_to, datetime.time(0, 0))
                > date_from
            )
            is_datetime_gt_date_from = isinstance(date_to, datetime.datetime) and date_to > date_from

            if date_to and (is_date_gt_date_from or is_datetime_gt_date_from):
                continue

            columns, data = get_data(
                table_name=table, column_name=date_column, date_from=date_from
            )

            write_old_data_to_s3(
                data=data,
                columns=columns,
                table_name=table,
                date_column=date_column,
                date_from=date_from,
                date_to=date_to,
            )
        raise_if_no_files_found()
        config.logger.info("archiving data to Glacier...")
        archive_old_data()
        config.logger.info("cleaning...")
        vacuum_db()
        clean_s3()
        config.logger.info("done")
    except Exception as e:
        config.logger.error(str(e))
        send_email(error=True, msg=str(e))
        return {"statusCode": 400, "body": "ERROR"}
    else:
        send_email()
        return {"statusCode": 200, "body": "OK"}


if __name__ == "__main__":
    lambda_handler({}, "")
