#!/usr/bin/env python
from collections import defaultdict
import csv
import pytest_benchmark
import json
import sys
import timeit
import xml.etree.ElementTree as ET
import xmltodict
from pymongo import MongoClient, ASCENDING

file_to_parse = sys.argv[1]

"""
This python module parse csv, txt or xml files in predefined schemas.
First you need to install few side libs: xmltodict- custom xml parser,
pymongo- MongoDB python client, pytest and pytest-benchmark for testing.
To do this you just need to run --> pip install -r requirements.txt
To use this parser just run --> python parser.py <your file_to_parse.xml, .csv or .txt>. 
File to parse must be in the same directory with parser.py.
There are two awailable xml_parser_version, you can choose "1" or "2"
"""

# default mongo db name
db_name = "products_db"

# choose "1" or "2" xml parser version
xml_parser_version = "1"


number_of_items = None

def parse_csv(file_name):
    global number_of_items
    # csv parser based on standard csv lib add id and address fields
    with open(file_name, "r") as file:
        reader = csv.reader(file)
        items_list = []
        address = None
        for item in reader:
            attrb_dict = {}
            for number, member in enumerate(item):
                attrb_dict[str(number)] = member
                try:
                    id = int(attrb_dict['0'].split('|')[0])
                    attrb_dict['id'] = id
                    if address is not None:
                        attrb_dict['address'] = address
                except ValueError:
                    address = attrb_dict['0']
                    attrb_dict = None
            if attrb_dict is not None:
                items_list.append(attrb_dict)
    number_of_items = len(items_list)
    print '{} item(s) parsed from {}'.format(len(items_list), file_to_parse)
    write_to_mongo_csv(items_list)

def parse_xml_2(file_name):
    global number_of_items
    # 2nd version of iterative parser based on xmldict lib 
    with open(file_name) as f:
        data = xmltodict.parse(f.read())
    items_list = data['DataFeeds']['item_data']
    number_of_items =  len(items_list)
    print '{} item(s) parsed from {}'.format(len(items_list), file_to_parse)
    write_to_mongo_xml(items_list)

def parse_xml_1(file_name):
    global number_of_items
    # 1st version of iterative parser based on standart xml lib 
    events = ("start", "end")
    context = ET.iterparse(file_name, events=events)
    items_list = recur(context)['DataFeeds']['item_data']
    number_of_items = len(items_list)
    print '{} item(s) parsed from {}'.format(len(items_list), file_to_parse)
    write_to_mongo_xml(items_list)

def create_db():
    client = MongoClient(port=27017)
    db = client[db_name]
    return db

def write_to_mongo_xml(data):
    # create mongo db and insert all data
    db = create_db()
    db.items.create_index([("id", ASCENDING)])
    counter = 0
    for item in data:
        item['id'] = item['item_basic_data']['item_unique_id']
        found = db.items.find({'id': item['id'] }).limit(1)
        if found.count() > 0:
            # older duplicates if founded are replaced by new item
            counter += found.count()
            result = db.items.delete_many({"id": item['id']})
        f = db.items.insert_one(item)
    print 'deleted {} duplicates'.format(counter)
    print '{} item(s) inserted into {}'.format(len(data), db_name)

def write_to_mongo_csv(data):
    # create mongo db and insert all data
    db = create_db()
    f = db.items.insert_many(data)
    print '{} item(s) inserted into {}'.format(len(data), db_name)

def recur(context, cur_elem=None):
    items = defaultdict(list)
    if cur_elem is not None:
        items.update(cur_elem.attrib)
    text = ""

    for action, elem in context:
        if action == "start":
            items[elem.tag].append(recur(context, elem))
        elif action == "end":
            text = elem.text.strip() if elem.text else ""
            break

    if len(items) == 0:
        return text

    return { k: v[0] if len(v) == 1 else v for k, v in items.items() }

def xml_benchmark_1():
    run_time = timeit.timeit("parse_xml_1(file_to_parse)", setup="from __main__ import parse_xml_1, file_to_parse", number=1)
    time = run_time * (1000000/number_of_items)
    size = 3.1 * (1000000/number_of_items)
    print 'file was parsed and stored into db at {} seconds'.format(run_time)
    print 'it will take near {} minutes to parse 1000000 products xml file with size {} megabytes'.format(time/60, size)
    print '{} seconds per item'.format(run_time/number_of_items)

def xml_benchmark_2():
    run_time = timeit.timeit("parse_xml_2(file_to_parse)", setup="from __main__ import parse_xml_2, file_to_parse", number=1)
    time = run_time * (1000000/number_of_items)
    size = 3.1 * (1000000/number_of_items)
    print 'file was parsed and stored into db at {} seconds'.format(run_time)
    print 'it will take near {} minutes to parse 1000000 products file xml with size {} megabytes'.format(time/60, size)
    print '{} seconds per item'.format(run_time/number_of_items)

def csv_benchmark():
    run_time = timeit.timeit("parse_csv(file_to_parse)", setup="from __main__ import parse_csv, file_to_parse", number=1)
    time = run_time * (1000000/number_of_items)
    size = 7.5 * (1000000/number_of_items)
    print 'file was parsed and stored into db at {} seconds'.format(run_time)
    print 'it will take near {} minutes to parse 1000000 products csv file with size {} megabytes'.format(time/60, size)
    print '{} seconds per item'.format(run_time/number_of_items)

def run_parser():
    if file_to_parse.split('.')[1] in ['txt', 'csv']: 
        parse_csv(file_to_parse)

    elif file_to_parse.split('.')[1] in ['xml']:

        func = globals()['parse_xml_{}'.format(xml_parser_version)]
        func(file_to_parse)

def run_benchmark():
    if file_to_parse.split('.')[1] in ['txt', 'csv']: 
        csv_benchmark()

    elif file_to_parse.split('.')[1] in ['xml']:

        func = globals()['xml_benchmark_{}'.format(xml_parser_version)]
        func()

if __name__ == "__main__":

    # run_parser()
    run_benchmark()
