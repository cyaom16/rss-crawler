from dateutil.parser import parse
from dateutil import tz
import sys
import mysql.connector as mdb


name = ''
try:
    name = sys.argv[1]
except Exception as e:
    print("Missing argument...")
    sys.exit()

print("Connecting to database...")
cnx = mdb.connect(host='localhost',
                  user='root',
                  password='yaochen',
                  database='test_db_{}'.format(name))
cur = cnx.cursor()

# UTC/GMT conversion
to_zone = tz.tzutc()

query = ''
if name == 'rss':
    query = "SELECT id, published_date FROM rss_feeds AND gmt_date IS NULL"
elif name == 'twitter':
    query = "SELECT tweetid, timestamp FROM fin_tech WHERE checked = 1 and validated = 1 AND gmt_date IS NULL"

try:
    cur.execute(query)
except Exception as e:
    print("Error: {}".format(e.args[1]))

rows = cur.fetchall()
print("Total # fetch results:", len(rows))
if not rows:
    print("No results fetched...")
    sys.exit()

gmt_dt = ''
for row in rows:
    if name == 'rss':
        gmt_dt = str(parse(row[1]).astimezone(to_zone)).split('+')[0]
        query = "UPDATE rss_feeds SET gmt_date = STR_TO_DATE(%s, '%Y-%m-%d %T') WHERE id = %s;"
    elif name == 'twitter':
        gmt_dt = row[1]
        query = "UPDATE fin_tech SET gmt_date = STR_TO_DATE(%s, '%Y-%m-%d %T') WHERE tweetid = %s;"
    try:
        cur.execute(query, (gmt_dt, row[0]))
        cnx.commit()
    except Exception as e:
        print("Error: {}".format(e.args[1]))
        # pass
    sys.stdout.write("\rProcess at: {:.1%}".format(rows.index(row) / len(rows)))
    sys.stdout.flush()

print("\nDone...")