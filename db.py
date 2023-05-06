import datetime

import psycopg2

import config


def get_archive_date() -> datetime:
    today = datetime.datetime.now()
    date = today - datetime.timedelta(days=config.DELTA_DAYS)
    # end of previous day
    return date.replace(hour=23, minute=59, second=59) - datetime.timedelta(1)


def connect():
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        database=config.DB_NAME,
    )
    return conn


def get_oldest_date(table_name: str, column_name: str) -> datetime:
    db = connect()
    cursor = db.cursor()
    try:
        query = (
            f"SELECT {column_name} FROM {table_name} order by {column_name} asc limit 1"
        )
        cursor.execute(query)
        date = cursor.fetchone()[0]
        config.logger.debug(f"Executed: <{query}> with result: {date}")
    except Exception as e:
        config.logger.error(str(e))
        return []
    finally:
        db.close()
    return date


def get_data(table_name: str, column_name: str, date_from: datetime) -> (list, list):
    db = None
    columns, data = [], []
    try:
        db = connect()
        cursor = db.cursor()
        query = f"SELECT * FROM {table_name} w WHERE w.{column_name} <= '{date_from}'"
        columns = list(map(lambda x: x[0], cursor.description))
        cursor.execute(query)
        data = cursor.fetchall()
        config.logger.debug(f"Executed: <{query}>")
    except Exception as e:
        config.logger.error(str(e))
    finally:
        if db is not None:
            db.close()
    return columns, data


def delete_data(table_name: str, column_name: str, date_from: datetime) -> None:
    db = connect()
    cursor = db.cursor()
    try:
        query = f"DELETE FROM {table_name} w WHERE w.{column_name} <= '{date_from}'"
        cursor.execute(query)
        db.commit()
        config.logger.debug(f"Executed: <{query}>")
    except Exception as e:
        config.logger.error(str(e))
    finally:
        db.close()


def vacuum_db() -> (list, list):
    db = connect()
    cursor = db.cursor()
    try:
        cursor.execute("COMMIT")
        cursor.execute("VACUUM")
        result = db.commit()
        config.logger.debug("VACUUM")
    except Exception as e:
        config.logger.error(str(e))
        return []
    finally:
        db.close()
    config.logger.info("")
    return result
