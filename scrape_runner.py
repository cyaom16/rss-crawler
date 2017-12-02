from rss_crawler import RSSCrawler
import os.path


data_path = 'data'
crawler = RSSCrawler(data_path=data_path)
crawler.extract_url(csv_path=os.path.join(crawler.root_path, 'feed_url.csv'))
crawler.run(timeout=7, to_db=True)
