#!/usr/bin/env python

import re

from bs4 import BeautifulSoup
# our cleandb has the html pretty tidy, really just want to strip out the <b>, <i>, and <br/> tags
def clean(input):
    output = input
    output = re.sub(r'<br/>',' ',output)
    output = re.sub(r'</?[bi]>','',output)
    return(output)

# load our "test" power
filename = 'testPower.txt'
content =  open(filename,'r').read()
content = clean(content)
# parse cleaned html
html = BeautifulSoup(content,'html.parser')
# first p element with class "powerstat"
keywordsTag = html.find_all(name='p',class_='powerstat')[0]

# # clean up the keywords a bit
keywords = re.sub(',','',keywordsTag.text.lower())
keywords = re.sub('^ | $ ','',keywords)
keywords = re.sub('\s+',' ',keywords)

print(keywords)
