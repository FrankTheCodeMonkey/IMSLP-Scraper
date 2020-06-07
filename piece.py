from html.parser import HTMLParser
import requests
import argparse
import ssl
from ssl import SSLContext

import arrow
import pika
import urllib

import csv


BASE = 'https://imslp.org{}'

def handle_piece(title, link):
    response = requests.get(link, verify=False)
    body = response.text

    csvfile = open('{}.csv'.format(title), 'w', newline='')

    fieldnames = ['ID', 'Link']
    csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_ALL)
    csv_writer.writeheader()

    parser = PieceParser(csv_writer)
    parser.feed(body)

    csvfile.close()

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
