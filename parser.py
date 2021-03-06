#!/usr/bin/env python
from collections import defaultdict
import csv
import memory_profiler
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
pymongo- MongoDB python client, pytest for testing.
To do this you just need to run --> pip install -r requirements.txt.
To use this parser just run --> python parser.py <your file_to_parse.xml, .csv or .txt>. 
File to parse must be in the same directory with parser.py.
There are two awailable xml_parser_version, you can choose "1" or "2".
If you want to benchmark memory usage then uncomment @profile decorator 
of run_parser() function and comment run_benchmark() function at the end of file,
then run -->
python -m memory_profiler parser.py <your file_to_parse.xml, .csv or .txt>
To run tests uncomment db_name = "pytest_db" and run --> 
python -m pytest -v pytest/test.py

"""

# default mongo db name
db_name = "products_db"

# test db_name
# db_name = "pytest_db"


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
    db.xml_items.create_index([("id", ASCENDING)])
    counter = 0
    for item in data:
        item['id'] = item['item_basic_data']['item_unique_id']
        found = db.xml_items.find({'id': item['id'] })
        if found.count() > 0:
            # older duplicates if founded are replaced by new item
            counter += found.count()
            result = db.xml_items.delete_many({"id": item['id']})
        f = db.xml_items.insert_one(item)
    print '{} item(s) inserted into {}'.format(len(data), db_name)
    print '{} duplicate(s) deleted'.format(counter)

def write_to_mongo_csv(data):
    # create mongo db and insert all data
    db = create_db()
    db.csv_items.create_index([("id", ASCENDING)])
    counter = 0
    for item in data:
        found = db.csv_items.find({'id': item['id'] })
        if found.count() > 0:
            # older duplicates if founded are replaced by new item
            counter += found.count()
            result = db.csv_items.delete_many({"id": item['id']})
        f = db.csv_items.insert_one(item)
    print '{} item(s) inserted into {}'.format(len(data), db_name)
    print '{} duplicate(s) deleted'.format(counter)

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

def benchmark(parser_type):
    run_time = timeit.timeit("parse_{}(file_to_parse)".format(parser_type), setup="from __main__ import parse_{}, file_to_parse".format(parser_type), number=1)
    time = run_time * (1000000/number_of_items)
    file_size = 3.1 if 'xml' in parser_type else 7.5 if 'csv' in parser_type  else 0
    size = file_size * (1000000/number_of_items)
    print 'file was parsed and stored into db at {} seconds'.format(run_time)
    print 'it will take near {} minutes to parse 1000000 products {} file with size {} megabytes'.format(time/60, parser_type[:3], size)
    print '{} seconds per item'.format(run_time/number_of_items)


# @profile
def run_parser():
    if file_ext in ['txt', 'csv']:
        parse_csv(file_to_parse)

    elif file_ext in ['xml']:
        func = globals()['parse_xml_{}'.format(xml_parser_version)]
        func(file_to_parse)

def run_benchmark():
    if file_ext in ['txt', 'csv']:
        benchmark('csv')

    elif file_ext == 'xml':
        benchmark('xml_{}'.format(xml_parser_version))

if __name__ == "__main__":

    file_ext = file_to_parse.split('.')[1]
    run_parser()
    run_benchmark()
