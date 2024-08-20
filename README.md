# indeed_scraper

A web scraper for the URL `https://www.indeed.com/jobs?q=[YOUR_TERM_HERE]&fromage=1`

## Requirements:

- RabbitMQ instance

## Installation

```
$ cd indeed_scraper/
$ . ./venv/bin/activate
$ pip install -r requirements.txt
$ celery -A main worker --loglevel=info
```

To send a search term for async scraping, follow these steps:

```
$ python send_message.py
```

## TO-DO

- Bypass CloudFlare human verification (critical)