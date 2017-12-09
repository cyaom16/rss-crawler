"""
Author: Matt Yao
Python 3.5 (OS X: 10.11.6)

Note: Require pyOpenSSL to connect some of the RSS website to avoid SSL handshake error

"""
from __future__ import absolute_import, print_function
import os
import csv
import time
import requests
import feedparser
import xml.etree.ElementTree as ET
import mysql.connector as mdb
from bs4 import BeautifulSoup


class RSSCrawler(object):
    def __init__(self, data_path):
        """ RSS feed crawler

        Arguments
            data_path: Path of XML data directory
        """
        assert isinstance(data_path, str)

        self.root_path = data_path

        self.targets = {'reuters_us':       'https://www.reuters.com/tools/rss',
                        'reuters_uk':       'https://uk.reuters.com/tools/rss',
                        'reuters_in':       'https://in.reuters.com/tools/rss',
                        'reuters_af':       'https://af.reuters.com/tools/rss',
                        'associated_press': 'http://hosted2.ap.org/APDEFAULT/APNewsFeeds',
                        'nytimes':          'https://www.nytimes.com/services/xml/rss/index.html',
                        'finextra':         'https://www.finextra.com/rss/rss.aspx'}

        self.sources = {'https://thefintechtimes.com/feed/': ('fintechtimes', 'FinTech'),
                        'https://www.betakit.com/feed': ('betakit', 'FinTech'),
                        'https://fintechweekly.com/feed.rss': ('fintechweekly', 'FinTech'),
                        'https://feeds.feedburner.com/finovate?format=xml': ('finovate', 'FinTech'),
                        'https://techcrunch.com/tag/fintech/feed/': ('techcrunch', 'FinTech'),
                        'https://news.google.com/news?cf=all&hl=en&pz=1&ned=us&q='
                        'fintech&output''=rss': ('google_news', 'FinTech')}

        self.xml_list = []

    def extract_url(self, csv_path):
        """ Web scrape RSS feeds URLs and save to CSV (run only once in the first time)

        Arguments
            csv_path: Path of the CSV stored the feed URLs
        """
        assert isinstance(csv_path, str)

        if os.path.isfile(csv_path):
            print("URL CSV already exits")
            return self.load_url(csv_path)

        for source in self.targets:
            try:
                res = requests.get(self.targets[source])
                assert res.status_code == 200
            except AssertionError:
                print("Cannot connect to {}, try again later".format(self.targets[source]))
                continue
            soup = BeautifulSoup(res.content, 'lxml')

            groups = None
            has_str = True
            has_cls = False

            if source == 'associated_press':
                groups = soup.find_all('div', 'rssmTblFrm', limit=1)
                has_cls = True
            elif source == 'nytimes':
                groups = soup.find_all('div', 'columnGroup doubleRule', limit=1)
                has_str = False
            elif source == 'finextra':
                groups = soup.find_all('table', limit=1)
            elif 'reuters' in source:
                groups = soup.find_all('div', 'module', limit=3)
                groups = groups[1:]

            for gp in groups:
                if source == 'finextra':
                    tags = gp.find_all('tr')
                else:
                    tags = gp.find_all('a', attrs={'href': True, 'class': has_cls}, string=has_str)
                if tags:
                    for t in tags:
                        if source == 'finextra':
                            td = t.find_all('td', limit=2)
                            category = td[0].get_text()
                            link = td[1].find('a').get('href')
                        else:
                            s = t.get_text().replace('\n', '').replace('/', '')
                            category = s.replace("'", '').replace('"', '').strip()
                            link = t.get('href')
                        # Update URL dictionary
                        self.sources[link] = (source, category)
                else:
                    print("-->No target tag found!")

            with open(csv_path, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=['URL', 'Source', 'Category'])
                writer.writeheader()
                for url in self.sources:
                    writer.writerow({'URL': url,
                                     'Source': self.sources[url][0],
                                     'Category': self.sources[url][1]})

        print("Found {} URls".format(len(self.sources)))
        return

    def load_url(self, csv_path):
        """ Load feed URLs from CSV

        Arguments
            csv_path: Path of the CSV stored the feed URLs
        """
        assert isinstance(csv_path, str)

        if os.path.isfile(csv_path):
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.sources[row['URL']] = (row['Source'], row['Category'])
            print("Total {} URLs".format(len(self.sources)))
        else:
            print("-->CSV path invalid!")

    def load_xml(self):
        self.xml_list = [os.path.join(root, f)
                         for root, _, files in os.walk(self.root_path)
                         for f in files
                         if f.endswith('.xml')]
        return self.xml_list

    def run(self, timeout, to_db=True):
        """ Scraper

        Arguments
            timeout: Running period
            to_db:   Load to database
        """
        # timeout = float(input("Please enter timeout period (day): "))
        timeout = float(timeout) * 3600 * 24
        time_start = time.time()
        time_end = time_start + timeout
        asctime_end = time.asctime(time.localtime(time_end))
        print("\nStarted at:", time.asctime(time.localtime(time_start)))
        print("Running for: {} min".format(timeout / 60))
        print("Terminated at:", asctime_end)

        while time.time() < time_end:
            time_left = (time_end - time.time()) / 60
            print("\nStarting crawler, time remaining: {:.1f} min ({:.1f} hrs)"
                  .format(time_left, time_left / 60))
            if to_db:
                try:
                    print("Connecting to database...")
                    cnx = mdb.connect(host='localhost',
                                      user='root',
                                      password='yaochen',
                                      database='test_db_rss')
                    cursor = cnx.cursor()
                except Exception as e:
                    print("-->Error: {}".format(e.args[1]))
                    print("-->Reconnecting in 30s...")
                    time.sleep(30)
                    continue

            for url in self.sources:
                num_update = 0
                # Unpack sources dictionary entry
                source, category = self.sources[url]
                # print(category, src)
                try:
                    # print("Connecting to {} ({})".format(src, category))
                    response = requests.get(url)
                    # assert response.status_code == 200
                except Exception:
                    print("-->Failed to connect to {0} ({1}), try again later"
                          .format(source, category))
                    time.sleep(5)
                    continue

                content = response.content
                # Remove leading newlines in the BetaKit XML feed
                if source == 'betakit':
                    content = content.decode('utf-8').replace('\n', '')

                res = feedparser.parse(content)

                xml_path = os.path.join(self.root_path, source, category + '.xml')
                if os.path.isfile(xml_path):
                    tree = ET.ElementTree()
                    tree.parse(xml_path)
                    data = tree.getroot()

                    # Retrieve the current sets of UIDs, titles in the XML file
                    uid_set = set(map(lambda i: i.text, data.iter('uid')))
                    title_set = set(map(lambda t: t.text, data.iter('title')))

                    for feed in res.entries:
                        # Some feed doesn't have ID key, we use link instead
                        try:
                            feed_id = feed.id
                        except AttributeError:
                            print("-->No <id> in {0} ({1}), try <link>".format(source, category))
                            try:
                                feed_id = feed.link
                            except AttributeError:
                                print("-->No <link> in {0} ({1}), ignore".format(source, category))
                                continue
                        try:
                            feed_title = feed.title.encode('ascii', 'ignore').decode('utf-8')
                        except AttributeError:
                            print("-->No <title> in {0} ({1}), ignore".format(source, category))
                            continue

                        if feed_id not in uid_set and feed_title not in title_set:
                            entry = ET.SubElement(data, 'entry')
                            uid = ET.SubElement(entry, 'uid')
                            title = ET.SubElement(entry, 'title')
                            link = ET.SubElement(entry, 'link')
                            summary = ET.SubElement(entry, 'summary')
                            published = ET.SubElement(entry, 'published_date')

                            uid.text = feed_id
                            title.text = feed_title
                            link.text = feed.link

                            try:
                                s = feed.summary
                            except AttributeError:
                                print("-->No <summary> in {0} ({1}), ignore"
                                      .format(category, source))
                                s = None
                            if s is None:
                                print("-->Empty <summary> at UID: {0} in {1} ({2}), try <content>"
                                      .format(feed_id, source, category))
                                try:
                                    s = feed.content[0].value
                                except AttributeError:
                                    print("-->No <content>")
                            # Remove HTML markup and non-ascii chars in summary
                            s = BeautifulSoup(s, 'lxml').get_text()
                            summary.text = s.encode('ascii', 'ignore').decode('utf-8')

                            try:
                                published.text = feed.published
                                gmt_tp = feed.published_parsed
                            except AttributeError:
                                print("-->No <published> in {0} ({1}), try <updated>"
                                      .format(source, category))
                                try:
                                    published.text = res.feed.updated
                                    gmt_tp = res.feed.updated_parsed
                                except AttributeError:
                                    print("-->No <updated> in {0} ({1}), use system GMT"
                                          .format(source, category))
                                    gmt_tp = time.gmtime()
                                    published.text = time.asctime(gmt_tp)
                            # UTC/GMT conversion %Y-%m-%d %T
                            # gmt_dt = str(parse(published.text).astimezone(to_zone)).split('+')[0]
                            gmt_dt = time.strftime('%Y-%m-%d %H:%M:%S', gmt_tp) if gmt_tp else ''

                            # Insert row into database table 'rss_feeds'
                            if to_db:
                                try:
                                    query = ("INSERT INTO rss_feeds"
                                             " (uid, title, link, summary, published_date,"
                                             " gmt_date, source, category)"
                                             " VALUE (%s, %s, %s, %s, %s,"
                                             " STR_TO_DATE(%s, '%Y-%m-%d %T'), %s, %s);")
                                    cursor.execute(query, (uid.text, title.text, link.text,
                                                           summary.text, published.text,
                                                           gmt_dt, source, category))
                                    cnx.commit()
                                except Exception as e:
                                    print("-->Error {}".format(e.args[1]))

                            num_update += 1

                    if num_update:
                        print("Found {0} update in {1} ({2})".format(num_update, source, category))
                        tree.write(xml_path)
                        time.sleep(0.5)
                else:
                    print("Creating a new XML file")

                    data = ET.Element('data')
                    for feed in res.entries:
                        entry = ET.SubElement(data, 'entry')
                        uid = ET.SubElement(entry, 'uid')
                        title = ET.SubElement(entry, 'title')
                        link = ET.SubElement(entry, 'link')
                        summary = ET.SubElement(entry, 'summary')
                        published = ET.SubElement(entry, 'published_date')

                        try:
                            uid.text = feed.id
                        except AttributeError:
                            print("-->No <id> in {0} ({1}), try <link>".format(source, category))
                            try:
                                uid.text = feed.link
                            except AttributeError:
                                print("-->No <link> in {0} ({1}), ignore".format(source, category))
                                continue
                        try:
                            title.text = feed.title.encode('ascii', 'ignore').decode('utf-8')
                        except AttributeError:
                            print("-->No <title> in {0} ({1}), ignore".format(source, category))
                            continue

                        link.text = feed.link

                        try:
                            s = feed.summary
                        except AttributeError:
                            print("-->No <summary> in {0} ({1}), ignore"
                                  .format(category, source))
                            s = None
                        if s is None:
                            print("-->Empty <summary> at UID: {0}, in {1} ({2}), try <content>"
                                  .format(uid.text, source, category))
                            try:
                                s = feed.content[0].value
                            except AttributeError:
                                print("-->No <content>")
                        # Remove HTML markup in the summary
                        s = BeautifulSoup(s, 'lxml').get_text()
                        summary.text = s.encode('ascii', 'ignore').decode('utf-8')

                        try:
                            published.text = feed.published
                        except AttributeError:
                            print("-->No <published> in {0} ({1}), try <updated>"
                                  .format(source, category))
                            try:
                                published.text = res.feed.updated
                            except AttributeError:
                                print("-->No <updated> in {0} ({1}), use system GMT"
                                      .format(source, category))
                                published.text = time.asctime(time.gmtime())

                        num_update += 1

                    tree = ET.ElementTree(data)
                    # num_update = len(res.entries)

                    tree.write(xml_path)
                    time.sleep(0.5)

                    if to_db:
                        try:
                            query = ("LOAD XML LOCAL INFILE '" + xml_path + "' INTO TABLE rss_feeds"
                                     " ROWS IDENTIFIED BY '<entry>' SET source=%s, category=%s;")
                            cursor.execute(query, (source, category))
                            cnx.commit()
                        except Exception as e:
                            print("Error {}".format(e.args[1]))

                    print("Found {0} update in {1} ({2})".format(num_update, source, category))

            if to_db:
                cursor.close()
                cnx.close()

            print("Now:", time.asctime(time.localtime()))
            print("Terminating at:", asctime_end)
            print("Waiting for 5 min\n")
            time.sleep(300)
