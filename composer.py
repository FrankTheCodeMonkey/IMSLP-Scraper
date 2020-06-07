from html.parser import HTMLParser
import requests
import argparse
import ssl
from ssl import SSLContext

import arrow
import pika
import urllib
import csv

from piece import handle_piece


BASE = 'https://imslp.org{}'
SITE = 'https://imslp.org'
COMPOSER_CATEGORY = '/wiki/Category:'
now = arrow.utcnow().timestamp

class PieceParser(HTMLParser):
    def __init__(self, csv_writer):
        HTMLParser.__init__(self)

        self.csv_writer = csv_writer
        self.existing_pages = set()
        self.PGLINK = 'external text'
        self.INTERNAL = 'internal'
        self.piece_id = []
        self.int_links = []

    def handle_starttag(self, tag, attrs):
        attribute = {a[0]: a[1] for a in attrs}
        href = attribute.get('href', None)
        title = attribute.get('title', None)
        attr_class = attribute.get('class', None)

        # Scrape the piece
        if tag == 'a':
            if attr_class == self.PGLINK and href and 'Special:ImagefromIndex' in href:
                piece_id = href.split('/')[-1]
                self.piece_id.append(piece_id)

            if attr_class == self.INTERNAL and href and title:
                self.int_links.append(urllib.parse.unquote(BASE.format(href)))

    def handle_endtag(self, tag):
        if tag == 'html':
            # Start writing shit
            for piece_id, piece_link in zip(self.piece_id, self.int_links):
                self.csv_writer.writerow({
                    'ID': piece_id,
                    'Link': piece_link
                })
            # exit()

class ComposerParser(HTMLParser):
    def __init__(self, csv_writer):
        HTMLParser.__init__(self)

        self.csv_writer = csv_writer
        self.existing_pages = set()
        self.PGLINK = 'categorypagelink'

    def handle_starttag(self, tag, attrs):
        attribute = {a[0]: a[1] for a in attrs}
        title = attribute.get('title', None)
        href = attribute.get('href', None)
        attr_class = attribute.get('class', None)

        # Scrape the piece
        if tag == 'a' and attr_class == self.PGLINK and title and href:
            link = urllib.parse.unquote(BASE.format(href))
            self.csv_writer.writerow({
                'Name': title,
                'Link': link
            })
            handle_piece(title, link)

        if attr_class == 'categorypaginglink' and 'pagefrom' in href and href not in self.existing_pages:
            self.existing_pages.add(href)
            scrape_piece_from_composer(SITE + href, self.csv_writer)


def imslp_ize_name(name):
    tokens = name.split()
    return '{},{}'.format(tokens[-1], ''.join(list(map(lambda x: '_{}'.format(x), tokens[:-1]))))


# def publish(everything):
#     connection = pika.BlockingConnection(
#     pika.ConnectionParameters(host='localhost'))
#     channel = connection.channel()

#     channel.queue_declare(queue='task_queue', durable=True)

#     for link in everything:
#         channel.basic_publish(
#             exchange='',
#             routing_key='task_queue',
#             body=link,
#             properties=pika.BasicProperties(
#                 delivery_mode=2,  # make message persistent
#             ))
#     connection.close()


def scrape_piece_from_composer(url, csv_writer):
    response = requests.get(url, verify=False)
    body = response.text
    parser = ComposerParser(csv_writer)
    parser.feed(body)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', help='composer name', required=True)
    args = parser.parse_args()
    composer = imslp_ize_name(args.name)

    # Defect: some of the fields arent quoted
    csvfile = open('{}-{}-Catalogue.csv'.format(now, composer), 'w', newline='')
    fieldnames = ['Name', 'Link']
    csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_ALL)
    csv_writer.writeheader()

    url = '{}{}{}'.format(SITE, COMPOSER_CATEGORY, 'Mendelssohn,_Felix')
    scrape_piece_from_composer(url, csv_writer)

    csvfile.close()
