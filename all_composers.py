from html.parser import HTMLParser
import requests
import argparse
import ssl
import urllib
import csv



BASE = 'https://imslp.org{}'
COMPOSERS = BASE.format('/wiki/Category:Composers')
PGLINK = 'categorysubcatlink'

exsting_pages = set()

class ComposerParser(HTMLParser):
    def __init__(self, csv_writer):
        HTMLParser.__init__(self)
        self.csv_writer = csv_writer

    def handle_starttag(self, tag, attrs):
        attribute = {a[0]: a[1] for a in attrs}
        # print('attrs', attribute)
        title = attribute.get('title', None)
        href = attribute.get('href', None)
        attr_class = attribute.get('class', None)

        # Scrape the piece
        if tag == 'a':
            if attr_class == PGLINK and title and href:
                # Category:Whittaker,_William_Gillies
                self.csv_writer.writerow({
                    'Name': title.split(':')[1],
                    'Link': urllib.parse.unquote(BASE.format(href))
                })

        # This should be a unique enough condition for nextpage
        if attr_class == 'categorypaginglink' and (href not in exsting_pages) and ('subcatfrom' in href):
            exsting_pages.add(href)
            do_scraping(BASE.format(href), self.csv_writer)


def do_scraping(link, csv_writer):
    response = requests.get(link, verify=False)
    body = response.text
    parser = ComposerParser(csv_writer)
    parser.feed(body)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='output file (csv)', required=True)
    args = parser.parse_args()

    # Defect: some of the fields arent quoted
    csvfile = open(args.file, 'w', newline='')
    fieldnames = ['Name', 'Link']
    csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    csv_writer.writeheader()


    do_scraping(COMPOSERS, csv_writer)

    csvfile.close()