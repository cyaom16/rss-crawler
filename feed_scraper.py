"""
Author: Matt Yao
Python 3.5 (OS X: 10.11.6)

Note: Require pyOpenSSL to connect some of the RSS website to avoid SSL handshake error

"""
from __future__ import absolute_import, print_function
import os
import sys
import csv
import time
import requests
import feedparser
import xml.etree.ElementTree as ET
import mysql.connector as mdb
from bs4 import BeautifulSoup
# from dateutil.parser import parse
# from dateutil import tz


# RSS URLs required to be scraped in the following sources
#
rss_sources = {'reuters_us': 'http://www.reuters.com/tools/rss',
               'reuters_uk': 'http://uk.reuters.com/tools/rss',
               'reuters_in': 'http://in.reuters.com/tools/rss',
               'reuters_af': 'http://af.reuters.com/tools/rss',
               'associated_press': 'http://hosted2.ap.org/APDEFAULT/APNewsFeeds',
               'nytimes': 'http://www.nytimes.com/services/xml/rss/index.html',
               'finextra': 'https://www.finextra.com/rss/rss.aspx'}

# source_list = rss_sources.keys()
# {url: (category, src)}
rss_urls = {'https://thefintechtimes.com/feed/': ('FinTech', 'fintechtimes'),
            'http://www.betakit.com/feed': ('FinTech', 'betakit'),
            'https://fintechweekly.com/feed.rss': ('FinTech', 'fintechweekly'),
            'http://feeds.feedburner.com/finovate?format=xml': ('FinTech', 'finovate'),
            'https://techcrunch.com/tag/fintech/feed/': ('FinTech', 'techcrunch'),
            'https://news.google.com/news?cf=all&hl=en&pz=1&ned=us&q=fintech&output=rss': ('FinTech', 'google_news')}

n = len(rss_urls)
# Begin to gather feed URLs
for src in rss_sources:
    csv_dir = './data/{}/feed_url.csv'.format(src)

    if not os.path.isfile(csv_dir):
        r = requests.get(rss_sources[src])
        # url = urlopen(rss_sources[src])
        # content = url.read()
        soup = BeautifulSoup(r.content, 'lxml')
        target_group = []
        has_str = True
        has_class = False

        if src == 'associated_press':
            target_group = soup.find_all('div', 'rssmTblFrm', limit=1)
            has_class = True
        elif src == 'nytimes':
            target_group = soup.find_all('div', 'columnGroup doubleRule', limit=1)
            has_str = False
        elif src == 'finextra':
            target_group = soup.find_all('table', limit=1)
        # For Reuters
        else:
            target_group = soup.find_all('div', 'module', limit=3)
            target_group = target_group[1:]

        csv_urls = {}
        for gp in target_group:
            if src == 'finextra':
                tags = gp.find_all('tr')
            else:
                tags = gp.find_all('a', attrs={'href': True, 'class': has_class}, string=has_str)
            # print(len(tags))
            if tags:
                for t in tags:
                    if src == 'finextra':
                        td = t.find_all('td', limit=2)
                        category = td[0].get_text()
                        link = td[1].find('a').get('href')
                    else:
                        category = t.get_text().replace('\n', '').replace('/', '').replace("'", '').replace('"', '')
                        link = t.get('href')
                    csv_urls[link] = (category, src)
            else:
                print("-->No target tag found!")

        print("# of URLs in {0} site: {1}".format(src, len(csv_urls)))

        with open(csv_dir, 'w') as f:
            fieldnames = ['Category', 'URL']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for url in csv_urls:
                writer.writerow({'Category': csv_urls[url][0], 'URL': url})

        rss_urls.update(csv_urls)
    else:
        with open(csv_dir, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rss_urls[row['URL']] = (row['Category'], src)
        print("# of URLs in {0} site: {1}".format(src, len(rss_urls) - n))
    n = len(rss_urls)
print("Total # of RSS URLs:", n)

# Input timer period
# timeout = float(input("Please enter timeout period (day): "))
timeout = float(sys.argv[1]) * 60 * 24
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
    print("\nStarting feed parser, time remaining: {:.1f} min ({:.1f} hrs)...".format(time_left, time_left / 60))
    try:
        print("Connecting to database...")
        cnx = mdb.connect(host='localhost',
                          user='root',
                          password='yaochen',
                          database='test_db_rss')
        cursor = cnx.cursor()
    except Exception as e:
        print("-->Error: {}".format(e.args[1]))
        print("Reconnecting in 30s...")
        time.sleep(30)
        continue

    for url in rss_urls:
        update_cnt = 0
        # (category, src)
        category = rss_urls[url][0]
        src = rss_urls[url][1]
        # print(category, src)
        try:
            # print("Connecting to {} ({})".format(src, category))
            r = requests.get(url)
            # print(r.status_code)
        except Exception:
            print("-->Failed to connect to {0} ({1}), try again later...".format(category, src))
            time.sleep(5)
            continue

        content = r.content
        # Remove leading newlines in the BetaKit XML feed
        if src == 'betakit':
            content = content.decode('utf-8').replace('\n', '')

        d = feedparser.parse(content)

        xml_dir = './data/{0}/{1}.xml'.format(src, category)
        if os.path.isfile(xml_dir):
            tree = ET.ElementTree()
            tree.parse(xml_dir)
            data = tree.getroot()
            # Retrieve the current sets of UIDs, titles in the XML file
            id_set = set(map(lambda i: i.text, data.iter('uid')))
            title_set = set(map(lambda i: i.text, data.iter('title')))
            for feed in d.entries:
                # Some feed doesn't have id key, we use link instead
                try:
                    fd_id = feed.id
                except AttributeError:
                    print("-->No <id> found in {0} ({1}), try <link>...".format(category, src))
                    # pass
                    try:
                        fd_id = feed.link
                    except AttributeError:
                        print("-->No <link> found in {0} ({1}), ignore...".format(category, src))
                        continue
                fd_title = feed.title.encode('ascii', 'ignore').decode('utf-8')
                if fd_id not in id_set and fd_title not in title_set:
                    entry = ET.SubElement(data, 'entry')
                    uid = ET.SubElement(entry, 'uid')
                    uid.text = fd_id

                    title = ET.SubElement(entry, 'title')
                    # Remove non-ascii chars in title
                    title.text = fd_title

                    link = ET.SubElement(entry, 'link')
                    link.text = feed.link

                    summary = ET.SubElement(entry, 'summary')
                    s = feed.summary
                    if s is None:
                        print("-->Empty <summary> found at UID: {0}, in {1} ({2}), \n"
                              "try <content>...".format(fd_id, category, src))
                        try:
                            s = feed.content[0].value
                        except AttributeError:
                            print("-->No <content>...")
                    # Remove HTML markup and non-ascii chars in summary
                    summary.text = BeautifulSoup(s, 'lxml').get_text().encode('ascii', 'ignore').decode('utf-8')

                    published = ET.SubElement(entry, 'published_date')
                    try:
                        published.text = feed.published
                        gmt_tp = feed.published_parsed
                    except AttributeError:
                        print("-->No <published> found in {0} ({1}), use <updated>...".format(category, src))
                        published.text = d.feed.updated
                        gmt_tp = d.feed.updated_parsed

                    # # UTC/GMT conversion %Y-%m-%d %T
                    # gmt_dt = str(parse(published.text).astimezone(to_zone)).split('+')[0]
                    gmt_dt = time.strftime('%Y-%m-%d %H:%M:%S', gmt_tp) if gmt_tp else ''

                    # Insert row into database table 'rss_feeds'
                    try:
                        query = ("INSERT INTO rss_feeds"
                                 " (uid, title, link, summary, published_date, gmt_date, category, source)"
                                 " VALUE (%s, %s, %s, %s, %s, STR_TO_DATE(%s, '%Y-%m-%d %T'), %s, %s);")
                        cursor.execute(query, (uid.text, title.text, link.text, summary.text, published.text,
                                               gmt_dt, category, src))
                        cnx.commit()
                    except Exception as e:
                        print("-->Error {}".format(e.args[1]))

                    update_cnt += 1

            if update_cnt:
                print("Found {0} update in {1} ({2})...".format(update_cnt, category, src))
                tree.write(xml_dir)
                time.sleep(0.5)
        else:
            print("Creating a new XML file...")
            data = ET.Element('data')
            for feed in d.entries:
                entry = ET.SubElement(data, 'entry')
                uid = ET.SubElement(entry, 'uid')
                try:
                    uid.text = feed.id
                except AttributeError as e:
                    print("-->No <id> found in {0} ({1}), use <link>...".format(category, src))
                    # pass
                    try:
                        uid.text = feed.link
                    except AttributeError:
                        print("-->No <link> found in {0} ({1}), ignore...".format(category, src))
                        continue

                title = ET.SubElement(entry, 'title')
                title.text = feed.title.encode('ascii', 'ignore').decode('utf-8')

                link = ET.SubElement(entry, 'link')
                link.text = feed.link

                summary = ET.SubElement(entry, 'summary')
                s = feed.summary
                if s is None:
                    print("-->Empty <summary> found at UID: {0}, in {1} ({2}), \n"
                          "try <content>...".format(uid.text, category, src))
                    try:
                        s = feed.content[0].value
                    except AttributeError as e:
                        print("-->No <content>...")
                # Remove HTML markup in the summary
                summary.text = BeautifulSoup(s, 'lxml').get_text().encode('ascii', 'ignore').decode('utf-8')

                published = ET.SubElement(entry, 'published_date')
                try:
                    published.text = feed.published
                except AttributeError as e:
                    print("-->No <published> found in {0} ({1}), use <updated>...".format(category, src))
                    published.text = d.feed.updated

            tree = ET.ElementTree(data)
            update_cnt = len(d.entries)

            tree.write(xml_dir)
            time.sleep(0.5)
            try:
                query = ("LOAD XML LOCAL INFILE '" + xml_dir + "' INTO TABLE rss_feeds"
                         " ROWS IDENTIFIED BY '<entry>' SET category = %s, source = %s;")
                cursor.execute(query, (category, src))
                cnx.commit()
            except Exception as e:
                print("Error {}".format(e.args[1]))

            print("Found {0} update in {1} ({2})...".format(update_cnt, category, src))

    cursor.close()
    cnx.close()

    print("Now:", time.asctime(time.localtime(time.time())))
    print("Terminated at:", asc_time_end)
    print("Waiting for 5 min...\n")
    time.sleep(300)
