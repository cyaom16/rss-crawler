import os
import time
import feedparser
import xml.etree.ElementTree as ET

import mysql.connector as mdb
from dateutil.parser import parse
from dateutil import tz
# UTC/GMT conversion
to_zone = tz.tzutc()


print("Connecting to database...")
cnx = mdb.connect(host='localhost',
                  user='root',
                  password='yaochen',
                  database='test_db_rss')
cursor = cnx.cursor()
for data_dir in os.listdir('./data/'):
    print("\nProcessing directory:", data_dir)
    try:
        for file in os.listdir('./data/' + data_dir):
            if file.endswith('.xml'):
                print("Processing:", file)
                xml_dir = './data/{0}/{1}'.format(data_dir, file)

                tree = ET.ElementTree()
                tree.parse(xml_dir)
                data = tree.getroot()

                id_set = set([i.text for i in data.iter('uid')])
                title_set = set([i.text for i in data.iter('title')])

                d = feedparser.parse('./data/{0}/tmp/{1}'.format(data_dir, file))
                src = data_dir
                category = file.replace('.xml', '')
                update_cnt = 0
                for feed in d.entries:
                    try:
                        fd_id = feed.uid
                    except AttributeError:
                        print("-->No <id> found in {0} ({1}), use <link>...".format(src, category))
                        pass
                    try:
                        fd_id = feed.link
                    except AttributeError:
                        print("-->No <link> found in {0} ({1}), ignore...".format(src, category))
                        continue
                    fd_title = feed.title
                    if fd_id not in id_set and fd_title not in title_set:
                        entry = ET.SubElement(data, 'entry')
                        uid = ET.SubElement(entry, 'uid')
                        uid.text = fd_id

                        title = ET.SubElement(entry, 'title')
                        title.text = fd_title

                        link = ET.SubElement(entry, 'link')
                        link.text = feed.link

                        summary = ET.SubElement(entry, 'summary')
                        summary.text = feed.summary

                        published = ET.SubElement(entry, 'published_date')
                        published.text = feed.published_date

                        # # UTC/GMT conversion %Y-%m-%d %T
                        gmt_dt = str(parse(published.text).astimezone(to_zone)).split('+')[0]

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
                    print("Found {0} update in {1} ({2})...".format(update_cnt, src, category))
                    tree.write(xml_dir)
                    time.sleep(0.5)

    except NotADirectoryError:
        print("Not a dir!")


cursor.close()
cnx.close()

