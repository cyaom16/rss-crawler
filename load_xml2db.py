import os
import time
import mysql.connector as mdb


cwd = os.getcwd()
#print(cwd)

cnx = mdb.connect(host='localhost',
                  user='root',
                  password='yaochen',
                  database='test_db_rss',
                  unix_socket='/tmp/mysql_default3059569.sock')
cursor = cnx.cursor()

for data_dir in os.listdir(cwd + '/data'):
    print("Processing directory:", data_dir)
    try:
        for file in os.listdir(cwd + '/data/' + data_dir):
            if file.endswith('.xml'):
                print("Processing {0} file:".format(file))
                xml_dir = cwd + '/data/' + data_dir + '/' + file
                category = file.replace('.xml', '')
                try:
                    query = ("LOAD XML LOCAL INFILE '" + xml_dir + "' INTO TABLE rss_feeds "
                             "ROWS IDENTIFIED BY '<entry>' SET category=%s, source=%s;")
                    cursor.execute(query, (category, data_dir))
                    cnx.commit()
                    time.sleep(0.5)
                except Exception as e:
                    print("Error {}".format(e.args[1]))
    except NotADirectoryError:
        print("Not a dir!")

cursor.close()
cnx.close()
