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
        """

        """
        assert isinstance(data_path, str)

        self.root_path = data_path

        self.targets = {'reuters_us':       'https://www.reuters.com/tools/rss',
                        'reuters_uk':       'https://uk.reuters.com/tools/rss',
                        'reuters_in':       'https://in.reuters.com/tools/rss',
                        'reuters_af':       'https://af.reuters.com/tools/rss',
                        'associated_press': 'https://hosted2.ap.org/APDEFAULT/APNewsFeeds',
                        'nytimes':          'https://www.nytimes.com/services/xml/rss/index.html',
                        'finextra':         'https://www.finextra.com/rss/rss.aspx'}

        self.sources = {'https://thefintechtimes.com/feed/': ('fintechtimes', 'FinTech'),
                        'https://www.betakit.com/feed': ('betakit', 'FinTech'),
                        'https://fintechweekly.com/feed.rss': ('fintechweekly', 'FinTech'),
                        'https://feeds.feedburner.com/finovate?format=xml': ('finovate', 'FinTech'),
                        'https://techcrunch.com/tag/fintech/feed/': ('techcrunch', 'FinTech'),
                        'https://news.google.com/news?cf=all&hl=en&pz=1&ned=us&q=fintech&output'
                        '=rss': ('google_news', 'FinTech')}
        self.xml_list = []

    def extract_url(self, csv_path):
        """
        Run only once in the first time

        """
        assert isinstance(csv_path, str)

        for source in self.targets:
            r = requests.get(self.targets[source])
            soup = BeautifulSoup(r.content, 'lxml')

            target_group = None
            has_str = True
            has_class = False

            if source == 'associated_press':
                target_group = soup.find_all('div', 'rssmTblFrm', limit=1)
                has_class = True
            elif source == 'nytimes':
                target_group = soup.find_all('div', 'columnGroup doubleRule', limit=1)
                has_str = False
            elif source == 'finextra':
                target_group = soup.find_all('table', limit=1)
            elif 'reuters' in source:
                target_group = soup.find_all('div', 'module', limit=3)
                target_group = target_group[1:]

            for gp in target_group:
                if source == 'finextra':
                    tags = gp.find_all('tr')
                else:
                    tags = gp.find_all('a',
                                       attrs={'href': True, 'class': has_class},
                                       string=has_str)

                if tags:
                    for t in tags:
                        if source == 'finextra':
                            td = t.find_all('td', limit=2)
                            category = td[0].get_text()
                            link = td[1].find('a').get('href')
                        else:
                            s = t.get_text().replace('\n', '').replace('/', '')
                            category = s.replace("'", '').replace('"', '')
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
        return self.sources

    def load_url(self, csv_path):
        """

        """
        assert isinstance(csv_path, str)

        if os.path.isfile(csv_path):
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.sources[row['URL']] = (row['Source'], row['Category'])
            return self.sources
        else:
            print("-->CSV path invalid!")

    def load_xml(self):
        self.xml_list = [os.path.join(root, f)
                         for root, _, files in os.walk(self.root_path)
                         for f in files if f.endswith('.xml')]
        return self.xml_list

    def scrape(self, timeout, to_db=True):
        # timeout = float(input("Please enter timeout period (day): "))
        timeout = float(timeout) * 60 * 24
        time_start = time.localtime()
        time_end = time.mktime(time_start) + timeout * 60
        asc_time_end = time.asctime(time.localtime(time_end))
        print("\nStarted at:", time.asctime(time_start))
        print("Running for: {} min".format(timeout))
        print("Terminated at:", asc_time_end)

        # UTC/GMT conversion
        # to_zone = tz.tzutc()

        while time.time() < time_end:
            time_left = (time_end - time.time()) / 60
            print("\nStarting scraper,"
                  "time remaining: {:.1f} min ({:.1f} hrs).".format(time_left, time_left / 60))
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

                source, category = self.sources[url]
                # print(category, src)
                try:
                    # print("Connecting to {} ({})".format(src, category))
                    r = requests.get(url)
                    assert r.status_code == 200
                except AssertionError:
                    print("-->Failed to connect to {0} ({1})\n"
                          "-->try again later...".format(source, category))
                    time.sleep(5)
                    continue

                content = r.content
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
                    id_set = set(map(lambda i: i.text, data.iter('uid')))
                    title_set = set(map(lambda t: t.text, data.iter('title')))
                    for feed in res.entries:
                        # Some feed doesn't have id key, we use link instead
                        try:
                            fd_id = feed.id
                        except AttributeError:
                            print("-->No <id> found in {0} ({1})\n"
                                  "-->try <link>...".format(source, category))
                            try:
                                fd_id = feed.link
                            except AttributeError:
                                print("-->No <link> found in {0} ({1})\n"
                                      "-->ignore...".format(source, category))
                                continue
                        fd_title = feed.title.encode('ascii', 'ignore').decode('utf-8')
                        if fd_id not in id_set and fd_title not in title_set:
                            entry = ET.SubElement(data, 'entry')
                            uid = ET.SubElement(entry, 'uid')
                            title = ET.SubElement(entry, 'title')
                            link = ET.SubElement(entry, 'link')
                            summary = ET.SubElement(entry, 'summary')
                            published = ET.SubElement(entry, 'published_date')

                            uid.text = fd_id
                            title.text = fd_title
                            link.text = feed.link
                            s = feed.summary
                            if s is None:
                                print("-->Empty <summary> found at UID: {0}, in {1} ({2})\n"
                                      "-->try <content>.".format(fd_id, source, category))
                                try:
                                    s = feed.content[0].value
                                except AttributeError:
                                    print("-->No <content>.")
                            # Remove HTML markup and non-ascii chars in summary
                            s = BeautifulSoup(s, 'lxml').get_text()
                            summary.text = s.encode('ascii', 'ignore').decode('utf-8')

                            try:
                                published.text = feed.published
                                gmt_tp = feed.published_parsed
                            except AttributeError:
                                print("-->No <published> found in {0} ({1})\n"
                                      "-->use <updated>.".format(source, category))
                                published.text = res.feed.updated
                                gmt_tp = res.feed.updated_parsed

                            # # UTC/GMT conversion %Y-%m-%d %T
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
                        print("Found {0} update in {1} ({2}).".format(num_update, source, category))
                        tree.write(xml_path)
                        time.sleep(0.5)
                else:
                    print("Creating a new XML file...")
                    data = ET.Element('data')
                    for feed in res.entries:
                        entry = ET.SubElement(data, 'entry')
                        uid = ET.SubElement(entry, 'uid')
                        title = ET.SubElement(entry, 'title')
                        link = ET.SubElement(entry, 'link')
                        published = ET.SubElement(entry, 'published_date')

                        try:
                            uid.text = feed.id
                        except AttributeError as e:
                            print("-->No <id> found in {0} ({1})\n"
                                  "-->use <link>...".format(source, category))
                            pass
                        try:
                            uid.text = feed.link
                        except AttributeError:
                            print("-->No <link> found in {0} ({1})\n"
                                  "-->ignore...".format(source, category))
                            continue

                        title.text = feed.title.encode('ascii', 'ignore').decode('utf-8')
                        link.text = feed.link
                        summary = ET.SubElement(entry, 'summary')
                        s = feed.summary
                        if s is None:
                            print("-->Empty <summary> found at UID: {0}, in {1} ({2})\n"
                                  "-->try <content>.".format(uid.text, source, category))
                            try:
                                s = feed.content[0].value
                            except AttributeError:
                                print("-->No <content>.")
                        # Remove HTML markup in the summary
                        s = BeautifulSoup(s, 'lxml').get_text()
                        summary.text = s.encode('ascii', 'ignore').decode('utf-8')
                        try:
                            published.text = feed.published
                        except AttributeError:
                            print("-->No <published> found in {0} ({1})\n"
                                  "-->use <updated>.".format(source, category))
                            published.text = res.feed.updated

                    tree = ET.ElementTree(data)
                    num_update = len(res.entries)

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

                    print("Found {0} update in {1} ({2}).".format(num_update, source, category))

            cursor.close()
            cnx.close()

            print("Now:", time.asctime(time.localtime(time.time())))
            print("Terminated at:", asc_time_end)
            print("Waiting for 5 min...\n")
            time.sleep(300)
