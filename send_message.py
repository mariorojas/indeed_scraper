import logging
import os
import time
from logging.handlers import RotatingFileHandler

import requests
from celery import Celery
from marshmallow import Schema, fields

# Config
CELERY_BROKER = 'pyamqp://guest@localhost//'
LOOP_TIME = 1200
MATCHING_URL = 'http://localhost:8000/api/matching'

# Logging
LOG_FOLDER_NAME = 'logs'
if not os.path.exists(LOG_FOLDER_NAME):
    os.mkdir(LOG_FOLDER_NAME)
LOG_FILE_NAME = LOG_FOLDER_NAME + '/' + 'console.log'
LOG_LEVEL = logging.DEBUG
LOG_BACKUP_COUNT = 5
LOG_MAX_BYTES = 1024 * 1024 * 16

log = logging.getLogger('bot')
log.setLevel(logging.DEBUG)

log_file_handler = RotatingFileHandler(
    filename=LOG_FILE_NAME,
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT,
)
log_file_handler.setLevel(LOG_LEVEL)

log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(LOG_LEVEL)

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_file_handler.setFormatter(log_formatter)
log_console_handler.setFormatter(log_formatter)

log.addHandler(log_file_handler)
log.addHandler(log_console_handler)


app = Celery('tasks', broker=CELERY_BROKER)


class MatchingSchema(Schema):
    position = fields.Str()
    prompt = fields.Str()
    user = fields.Integer()


@app.task(name='pull_jobs')
def pull_jobs(term):
    return term


def main():
    log.info('Retrieving matching terms from %s', MATCHING_URL)
    response = requests.get(MATCHING_URL)
    response.raise_for_status()

    matching = MatchingSchema(many=True).load(response.json())

    terms = {entry['position'].lower() for entry in matching}
    num_of_terms = len(terms)

    log.debug('Unique terms for scraping: %s', num_of_terms)

    for raw_term in terms:
        log.info('Sending term "%s" to RabbitMQ', raw_term)
        pull_jobs.delay(raw_term)


if __name__ == '__main__':
    while True:
        start = time.perf_counter()

        main()

        end = int(time.perf_counter() - start)
        log.info('Complete after %ss', end)

        log.info('Sleeping %s...', LOOP_TIME)
        time.sleep(LOOP_TIME)
