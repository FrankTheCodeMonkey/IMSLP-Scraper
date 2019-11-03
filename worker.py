#!/usr/bin/env python
import pika
import time
import requests
import traceback

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
import arrow
import logging

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', heartbeat=600))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def save(url):
    print('File: ', url)
    response = requests.get(url, verify=False)
    filename = url.split('/')[-1]
    open(filename, 'wb').write(response.content)
    print(f'Downloaded {filename} successfully')

def rediects_to_file(url):
    return url.split('.')[-1].lower() in {'mp3', 'pdf', 'zip'}

def process_single(link):
    # Set up a firefox browser
    options = Options()
    options.headless = True

    browser = webdriver.Firefox(options=options, executable_path='/usr/local/bin/geckodriver')
    browser.set_page_load_timeout(10)

    print(f'Processing: {link}')

    # A timeout a day keeps the prod issue away
    try:
        browser.get(link)
        if rediects_to_file(browser.current_url):
            save(browser.current_url)
    except Exception as e:
        # TODO: what happens if the file is readily available?
        print('failed to get resource {}'.format(browser.current_url))
        print(traceback.print_exc())
    finally:
        browser.quit()

    # Lets wait 16 seconds just be safe
    pdffile = None
    try:
        WebDriverWait(browser, 16).until(
            expected_conditions.presence_of_element_located((By.ID, 'sm_dl_wait'))
        )
    except Exception as e:
        if rediects_to_file(browser.current_url):
            save(browser.current_url)

        # TODO: what happens if the file is readily available?
        print('Exception waiting for webdriver\n', e)
        print(traceback.print_exc())
    finally:
        browser.quit()

    try:
        url = browser.find_element_by_id('sm_dl_wait').get_attribute('data-id')
        save(url)
    except Exception as e:
        print('Exception downloading for file\n', e)
    finally:
        browser.quit()


def callback(ch, method, properties, body):
    link = body.decode("utf-8")
    print(" [x] Received %r" % link)
    process_single(link)
    print(" [x] Done at %r" % arrow.utcnow().timestamp)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()
