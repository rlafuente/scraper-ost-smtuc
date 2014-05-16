#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import urllib2
import scraperwiki

# TODO:
# Opção pra omitir a segunda entrada da mesma paragem
#   é verificável pelo stop_code

API_RESOURCE = "stops"
AGENCY_ID = 8 # SMTUC
API_KEY = "___REMOVED___"
API_URL_BASE = 'https://api.ost.pt'
API_URL = '%s/%s/?agency=%d&withroutes=false&key=%s' % (API_URL_BASE, API_RESOURCE, AGENCY_ID, API_KEY)

os.environ["SCRAPERWIKI_DATABASE_NAME"] = "data.sqlite"
i = 1

def scrape(local=False):
    if not local:
        print "Generating SQLite output from the OST.pt API..."
        generate_db_from_api(API_URL)
        # make sure the DB name conforms to morph.io
        if os.path.exists('scraperwiki.sqlite') and not os.path.exists('data.sqlite'):
            os.rename('scraperwiki.sqlite', 'data.sqlite')
    else:
        generate_db_from_files()

def data2sqlite(data):
    for o in data['Objects']:
        scraperwiki.sqlite.save(unique_keys=['id'], table_name="data",
                                                    data={"id": o["id"], 
                                                          "stop_code": o["stop_code"],
                                                          "stop_desc": o["stop_desc"],
                                                          "stop_name": o["stop_name"],
                                                          "parent_station": o["parent_station"],
                                                          "parent_station_id": o["parent_station_id"],
                                                          "lat": o["point"]["coordinates"][0],
                                                          "lon": o["point"]["coordinates"][1],
                                                          })

def generate_db_from_api(url, save_json_files=False):
    global i
    response = urllib2.urlopen(url)
    r = response.read()
    data = json.loads(r)
    data2sqlite(data)

    # this should only happen on the local scraper, not on Morph
    if save_json_files:
        filename = "smtuc-stops-%02d.json" % i
        f = open(filename, 'w')
        f.write(r)
        f.close()
    i += 1

    # are there more results?
    if not data['Meta'].get('next_page'):
        return
    next_url = API_URL_BASE + data['Meta']['next_page']
    print next_url
    generate_db_from_api(next_url)

def generate_db_from_files():
    print "Generating SQLite output from downloaded files..."
    from glob import glob
    files = glob("./smtuc*.json")
    for f in files:
        r = open(f, 'r').read()
        data = json.loads(r)
        data2sqlite(data)
        print f

scrape()
# scrape(local=True)

