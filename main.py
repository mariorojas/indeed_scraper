import datetime
import logging
import os
import random
import time
from logging.handlers import RotatingFileHandler
from urllib.parse import quote

from celery import Celery
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Config
CELERY_BROKER = 'pyamqp://guest@localhost//'
JOBS_URL = 'https://www.indeed.com/jobs?q={0}&fromage=1'
MATCHING_URL = 'http://localhost:8000/api/matching'
PRODUCTION = True
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) KHTML, like Gecko'

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


def process_html_container(browser, container):
    title = container.find_element(by=By.CSS_SELECTOR, value='.jobTitle span').text

    link = container.find_element(by=By.CSS_SELECTOR, value='.jobTitle a')
    url = link.get_attribute('href')

    link.click()

    # random sleep emulates real user interaction
    sleep = random.randint(2, 8)
    log.debug('Waiting %ss before reading job detail. URL %s', sleep, url)

    time.sleep(sleep)

    external_id = link.get_attribute('data-jk')

    description_containers = browser.find_elements(by=By.CSS_SELECTOR, value='#jobDescriptionText')
    description = description_containers[0].text

    company_container = container.find_element(by=By.CSS_SELECTOR, value='.company_location')
    company = company_container.find_element(by=By.CSS_SELECTOR, value="span[data-testid='company-name']").text
    location = company_container.find_element(by=By.CSS_SELECTOR, value="div[data-testid='text-location']").text

    if location.startswith('Remote'):
        job_type = 'rmt'
    elif location.startswith('Hybrid'):
        job_type = 'hyb'
    else:
        job_type = 'onst'

    tags = []
    salary = None
    salary_job_type_containers = browser.find_elements(by=By.CSS_SELECTOR, value='#salaryInfoAndJobType span')
    for _ in salary_job_type_containers:
        salary_job_type_text = _.text
        if '$' in salary_job_type_text:
            salary = salary_job_type_text
        else:
            tag = salary_job_type_text.replace('- ', '')
            tags.append(tag)

    item_containers = browser.find_elements(by=By.CSS_SELECTOR, value="div[data-testid='list-item']")
    for _ in item_containers:
        tag = _.text
        tags.append(tag)

    log.debug('Info successfully retrieved from URL %s', url)

    return {
        'company': company,
        'description': description,
        'external_id': external_id,
        'job_type': job_type,
        'location': location.replace('\n', ''),
        'salary': salary,
        'tags': tags,
        'title': title,
        'url': url,
    }


@app.task(name='pull_jobs')
def pull_jobs(raw_term):
    start = time.perf_counter()

    log.debug('Processing term "%s"', raw_term)
    term = quote(raw_term)

    options = Options()
    options.add_argument('--disable-gpu')

    if PRODUCTION:
        options.add_argument('--headless')
        options.add_argument(f'--user-agent={USER_AGENT}')

    # web drivers are located in $HOME/.cache/selenium
    browser = webdriver.Chrome(options=options)

    url = JOBS_URL.format(term)
    log.debug('Scraping %s', url)

    browser.get(url)

    # wait for cloudflare verification
    time.sleep(10)

    containers = browser.find_elements(by=By.CSS_SELECTOR, value='.job_seen_beacon')
    num_of_containers = len(containers)
    log.debug('Containers to process: %s. URL %s', num_of_containers, url)

    if num_of_containers == 0:
        log.warning('No results found for term "%s". URL %s', term, url)
        date_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        file_name = f'{LOG_FOLDER_NAME}/no-containers-{date_time}.png'
        browser.get_screenshot_as_file(file_name)

    jobs = []

    for container in containers:
        try:
            job = process_html_container(browser, container)
            jobs.append(job)
        except NoSuchElementException as err:
            date_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            file_name = f'{LOG_FOLDER_NAME}/no-element-{date_time}.png'
            browser.get_screenshot_as_file(file_name)
            log.error('HTML Element not found for term "%s". Error: %s', term, err)
        except Exception as err:
            date_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            file_name = f'{LOG_FOLDER_NAME}/error-{date_time}.png'
            browser.get_screenshot_as_file(file_name)
            log.error('An unexpected error occurred for term "%s"', term, err)

    browser.quit()

    num_of_jobs = len(jobs)
    log.info('Collected jobs for term "%s": %s', term, num_of_jobs)

    for job in jobs:
        log.debug(job)

    end = int(time.perf_counter() - start)
    log.info('Complete scraping for term "%s" after %ss', term, end)
