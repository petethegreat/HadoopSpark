#!/usr/bin/env python

import sqlite3
import re,sys
from bs4 import BeautifulSoup

# get the keywords from the html
def getKeywords(descRaw):
    content = re.sub(r'<br/>',' ',descRaw)
    content = re.sub(r'</?[bi]>','',content)
    html = BeautifulSoup(content,'html.parser')
    keywordsTag = html.find_all(name='p',class_='powerstat')[0]
    keywords = re.sub(',','',keywordsTag.text.lower())
    keywords = re.sub('^ | $ ','',keywords)
    keywords = re.sub('\s+',' ',keywords)
    return(keywords)

# filenames
cleanDBname = './cleanedDB.db'
outfileName = './powerKeywords.txt'

# connect to DB
cleanConn = sqlite3.connect(cleanDBname)
cleanConn.row_factory = sqlite3.Row
cleancur = cleanConn.cursor()

# query
cleanquery= 'SELECT id,classname,level,descriptionRaw FROM Powers'
cleancur.execute(cleanquery)

# hold the output here, then write it
# db is small, so memory won't be an issue, otherwise we could write each line as we retreive its row
lines = []

# loop over query results (rows)
for therow in cleancur:
    keywords = getKeywords(therow['descriptionRaw'].encode('utf-8'))
    # print('id = {id:5}, name = {name}, keywords = {keywords}'.format(id=therow['id'],name=therow['name'],keywords=keywords))
    
    # Some powers don't have a valid level associated with them
    # (and the db contains garbage level values, HASHXX - perl related?)
    try:
        levelint = int(therow['level'])
    except ValueError:
        continue
    try:
        lines.append(
            '{classname}_{level} {keywords}\n'.format(
                classname=re.sub(r' ','_',therow['classname'].encode('utf-8')),
                level=therow['level'],
                keywords=keywords.encode('utf-8')
                )
            )
    except UnicodeEncodeError:
        print('UnicodeError : powerid = {id}\n'.format(id=therow['id']))
        sys.exit()
cleancur.close()


try:
    with open(outfileName,'w') as outfile:
        for line in lines:
            outfile.write(line)
except IOError:
    print('Error, could not open {outfilename} for writing'.format(outfilename=outfileName))
    sys.exit()

