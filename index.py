from html.parser import HTMLParser
import requests
import argparse
import ssl
from ssl import SSLContext

import arrow
import pika


SITE = 'https://imslp.org'

COMPOSER_CATEGORY = '/wiki/Category:'

COMPOSER = ''

everything = set()

def handle_piece(attribute):
    print('Link: {}'.format(SITE + attribute['href']))
    start_scraping(SITE + attribute['href'], PieceParser)

def handle_individual_doc(attribute):
    pass

def start_scraping(url, parser):
    response = requests.get(url, verify=False)
    body = response.text
    parser = parser()
    parser.feed(body)

class PieceParser(HTMLParser):
    downloaded_again = False
    def handle_starttag(self, tag, attrs):
        # Scrape the piece
        attribute = {}
        if tag == 'a':

            for a in attrs:
                attribute[a[0]] = a[1]

                href = attribute.get('href', '')
                cLass = attribute.get('class', '')

            if 'Special:ImagefromIndex' in href:
                print('Piece Link: {}'.format(href))
                piece_id = href.split('/')[-1]
                everything.add(f'http://imslp.org/wiki/Special:IMSLPDisclaimerAccept/{piece_id}')
        print(attribute)


class ComposerParser(HTMLParser):
    total = 0
    count = 0

    handled_next = False
    def handle_starttag(self, tag, attrs):
        # Scrape the piece
        if tag == 'a':
            attribute = {}

            for a in attrs:
                attribute[a[0]] = a[1]

            href = attribute.get('href', '')
            cLass = attribute.get('class', '')

            if ComposerParser.count == ComposerParser.total:
                return

            is_piece = href.startswith('/wiki/') and href.endswith(f'({COMPOSER})')
            if is_piece:
                handle_piece(attribute)
                ComposerParser.count += 1

            # This should be a unique enough condition for nextpage
            if cLass == 'categorypaginglink' and href.endswith('#mw-pages') and 'pagefrom' in href:
                print('next page', ComposerParser.count, ComposerParser.total)
                ComposerParser.handled_next = True
                start_scraping(SITE + href, ComposerParser)

    def handle_endtag(self, tag):
        if tag == 'html':
            print('metrics', ComposerParser.count, ComposerParser.total)
            ComposerParser.handled_next = False

    def handle_data(self, data):
        # No next page
        if data.startswith('Compositions ('):
            ComposerParser.total = int(data.split('(')[-1][:-1])

        # The pagination button becomes a text, so we can stop here
        if data.startswith(') (next'):
            ComposerParser.handled_next = True

def imslp_ize_name(name):
    tokens = name.split()
    return '{},{}'.format(tokens[-1], ''.join(list(map(lambda x: '_{}'.format(x), tokens[:-1]))))


def publish(everything):
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)

    for link in everything:
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=link,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        print(" [x] Sent %r" % link)
    connection.close()


if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--composer', help='composer name', required=True)
    # args = parser.parse_args()
    # COMPOSER = imslp_ize_name(args.composer)
    # url = f'{SITE}{COMPOSER_CATEGORY}{COMPOSER}'

    # # Working
    # url = 'https://imslp.org/wiki/6_Violin_Sonatas_and_Partitas,_BWV_1001-1006_(Bach,_Johann_Sebastian)'

    # # TODO: Ysaye - scrape imslp.eu
    # url = 'https://imslp.org/wiki/6_Sonatas_for_Solo_Violin%2C_Op.27_(Ysa%C3%BFe%2C_Eug%C3%A8ne)'

    # # Beethoven TODO: scrape the other tabs
    # # Update: okay so it's actually geting everything at once. Nice.
    url = 'https://imslp.org/wiki/Symphony_No.1,_Op.68_(Brahms,_Johannes)'

    start_scraping(url, PieceParser)
    now = arrow.utcnow().timestamp
    print(f'started at {now}')
    publish(everything)