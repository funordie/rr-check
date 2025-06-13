import sys
import requests
import json

from lxml import html

import requests
from bs4 import BeautifulSoup

import csv

print ('Number of arguments:', len(sys.argv), 'arguments.')
print ('Argument List:', str(sys.argv))

DETAIL_URL="https://ntr.tourism.government.bg/CategoryzationAll.nsf/detail.xsp"
# https://ntr.tourism.government.bg/CategoryzationAll.nsf/api/data/collections/name/vRegistarMNValid1?sortcolumn=CNumber&sortorder=ascending&ps=100&page=0&_=1749322795422"
URL = "https://ntr.tourism.government.bg/CategoryzationAll.nsf/api/data/collections/name/vRegistarMNValid1"
# {"@href": "/CategoryzationAll.nsf/api/data/collections/name/vRegistarMNValid1/unid/AB52DF2D56301968C22585D20082F162",
#   "@link": 
# {"rel": "document", 
#  "href": "/CategoryzationAll.nsf/api/data/documents/unid/AB52DF2D56301968C22585D20082F162"}, 
# "@entryid": "1-AB52DF2D56301968C22585D20082F162", 
# "@unid": "AB52DF2D56301968C22585D20082F162", 
# "@noteid": "5C66A", 
# "@position": "1", "@read": true, "@siblings": 33867, "@form": "fProcedure", "CNumber": "00001", 
# "TOSubType1": "Къща за гости", "TOName": "КЪЩА ЗА ГОСТИ \"БЛАГОВЕСТА\"", "TOCity": "Илинденци", 
# "TOAddress": "ул.Вихрен №4", "CategoryGiven": "1", "TORoomsGiven": 10, "TOBedsGiven": 24, "TOMunicipality": "Струмяни", 
# "CCOrderNumber": "З-242", "CCOrderDate": "2020-07-31T09:00:00Z", "CValidityDate": "2025-07-31T09:00:00Z", "CCValidityDate": "2025-07-31T09:00:00Z", 
# "StopDate": "", "StopTo": "", "UNID": "4100D8DA4B8E450BC22585C1002D1477", "Prefix": "с.", "FormDB": "2", "Form": "fProcedure", "UIKNumber": "СЧ-ГР8-7ЧР-1П"}

page_cnt = 0
main_loop = 200
# unid_list = []

csv_sep = ','
csvfile = open('csvfile.csv', 'w', newline='', encoding='utf-8')
# make a new variable - c - for Python's CSV writer object -
c = csv.writer(csvfile)

# write a column headings row - do this only once -
c.writerow( ['pos','link','name','sobstvenik','stopanisvast','data'] )

while main_loop == 200 :
    PARAMS = {'sortcolumn':'CNumber', 'sortorder':'ascending', 'ps':'100', 'page':page_cnt}
    #print("DEBUG: URL: " + URL)
    #print("DEBUG: PARAMS: ", PARAMS)
    # sending get request and saving the response as response object

    print ("page_cnt:",page_cnt)
    main_responce = requests.get(url=URL, params=PARAMS)
    main_loop = main_responce.status_code

    # extracting data in json format
    root_data = main_responce.json()
    root_len = len(root_data)
    if root_len == 0:
        print("break at page_cnt:", page_cnt) 
        break

    for item in root_data:
        print ('pos', item['@position'], 'id:', item['@unid'])
        uid = item['@unid']
        pos = item['@position']
        name = item['TOName']

        DETAIL_PARAM = {'id':uid}
        page = requests.get(url=DETAIL_URL, params=DETAIL_PARAM)
        print("DEBUG: detail response status: " , page.status_code)

        link = DETAIL_URL+"?id="+uid
        soup = BeautifulSoup(page.content, "html.parser")

        sobstvenik = soup.find(id="view:_id1:computedField30").get_text()
        # print(sobstvenik)

        stopanisvast = soup.find(id="view:_id1:computedField31").get_text()
        # print(stopanisvast)

        data = soup.find(class_="tab-content p30").get_text()
        # print(data.strip())

        # f.write(pos + csv_sep + uid + csv_sep + name + csv_sep + sobstvenik + csv_sep + stopanisvast + csv_sep + data + '\n')
        rowdata = [pos, link, name, sobstvenik, stopanisvast, data]
        print(rowdata)
        c.writerow(rowdata)
        # main_loop = 0
    print("DEBUG: main response status: " , main_responce.status_code)
    page_cnt += 1

csvfile.close()
print("finish scraper")
