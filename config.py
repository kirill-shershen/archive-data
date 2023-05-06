import logging
import os
import sys

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(os.path.join(CURRENT_DIR, "lib"))

from sshtunnel import SSHTunnelForwarder


LOGGER_NAME = "EVENTS-ARCHIVE"
loglevel = (
    logging.DEBUG if os.environ.get("LOGLEVEL", "INFO") == "DEBUG" else logging.INFO
)


def get_logger():
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(loglevel)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(module)s(%(lineno)d) - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(ch)
    return logger


logger = get_logger()

SSH_REMOTE_HOST = os.environ.get("SSH_HOST", "")
SSH_REMOTE_PORT = 22
SSH_REMOTE_USERNAME = os.environ.get("SSH_USER", "")
SSH_KEY = os.path.join(CURRENT_DIR, "cu-ecs-qa.cer")


DB_HOST = os.environ.get("DB_HOST", "")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_USER = os.environ.get("DB_USER", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "")
env = os.environ.get("ENV", "local")
local = env == "local"
DELTA_DAYS = int(os.environ.get("DELTA_DAYS", 365 * 4))

# port forwarding if needed
if local:
    SSH_SERVER = SSHTunnelForwarder(
        (SSH_REMOTE_HOST, SSH_REMOTE_PORT),
        ssh_username=SSH_REMOTE_USERNAME,
        ssh_pkey=SSH_KEY,
        remote_bind_address=(DB_HOST, DB_PORT),
        local_bind_address=("0.0.0.0", 5431),
    )
    logger.debug("starting ssh tunnel...")
    SSH_SERVER.start()
    logger.debug("ssh tunnel connected")

# AWS S3
S3_BUCKET = os.environ.get("S3_BUCKET", "cu-qa-events")
S3_ARCHIVE = os.environ.get("S3_ARCHIVE", "archive/")
S3_SETTINGS = os.environ.get("S3_SETTINGS", "config")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
GLACIER_VAULT = os.environ.get("GLACIER_VAULT", "events_test")

# Email
EMAIL_SENDER = os.environ.get("EMAIL_SENDER", "")
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT", "")
