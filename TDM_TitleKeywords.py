#Copyright [2013] [Dmitry Efimov, Lucas Silva, Ben Solecki ]

#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#http://www.apache.org/licenses/LICENSE-2.0
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

import csv
import json
import os
import pickle
import psycopg2
import textmining
import re

def get_paths():
    paths = json.loads(open("SETTINGS.json").read())
    for key in paths:
        paths[key] = os.path.expandvars(paths[key])
    return paths

conn_string = None

def get_db_conn():
    global conn_string
    if conn_string is None:
        conn_string = get_paths()["postgres_conn_string"]
    if "##AskForPassword##" in conn_string:
        password = raw_input("PostgreSQL Password: ")
        conn_string = conn_string.replace("##AskForPassword##", password)
    conn = psycopg2.connect(conn_string)
    return conn

def create_keyword_table():
	conn = get_db_conn()
	query = "SELECT * FROM Paper"
	cursor = conn.cursor()
	cursor.execute(query)
	res = cursor.fetchall()

	tdm = textmining.TermDocumentMatrix()

	for x in res:
		tdm.add_doc(re.sub("[^A-Za-z0-9 _]"," ",' '.join([str(x[1]),str(x[5])])))

	keywords = []
	cutoff = 30
	stopwords = textmining.stopwords
	stopwords.update(['key', 'words', 'keywords', 'keyword', 'word'])
	for i in xrange(len(tdm.sparse)):
		id_paper = res[i][0]
		year = res[i][2]
		id_conference = res[i][3]
		id_journal = res[i][4]
		paper_words = [[id_paper,year,id_conference,id_journal,word] for word in tdm.sparse[i].keys() if tdm.doc_count[word] >= cutoff and word not in stopwords]
		keywords.extend(paper_words)

	cursor.execute("DROP TABLE IF EXISTS keywords")
	cursor.execute("CREATE TABLE keywords(paperid INT, year INT, conferenceid INT, journalid INT, keyword TEXT)")
	query = "INSERT INTO keywords (paperid, year, conferenceid, journalid, keyword) VALUES (%s, %s, %s, %s, %s)"
	cursor.executemany(query, keywords)
	conn.commit()
	conn.close()

if __name__ == "__main__":
    create_keyword_table()

#writer = csv.writer(open("temp.csv", "w"), lineterminator="\n")
#writer.writerow(("PaperId", "Keyword"))
#writer.writerows(keywords)
