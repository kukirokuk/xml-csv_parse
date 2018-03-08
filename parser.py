#!/usr/bin/env python
from collections import defaultdict
import csv
import json
import sys
import xml.etree.ElementTree as ET
import xmltodict
from pymongo import MongoClient

file_to_parse = sys.argv[1]
# import pdb; pdb.set_trace()

"""
This python module parse csv, txt or xml files in predefined schemas
To use this parser just run python parser.py <your file_to_parse.xml, .csv or .txt> 
that must be in the same directory with parser.py.
There are two awailable xml parser versions, you can choose "1" or "2"
To use it you also need to pip install two side libs: xmltodict- custom xml parser
and pymongo- MongoDB python client.
"""

# default mongo db name
db_name = "products_db"

# choose "1" or "2" xml parser version
xml_parser_version = "1"

def parse_csv_1(file_name):

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
    write_to_mongo(items_list)
    print '{} item(s) parsed from {}'.format(len(items_list), file_to_parse)

def parse_xml_2(file_name):
    # 2nd version of iterative parser based on xmldict lib 
    with open(file_name) as f:
        data = xmltodict.parse(f.read())
    items_list = data['DataFeeds']['item_data']
    write_to_mongo(items_list)
    print '{} item(s) parsed from {}'.format(len(items_list), file_to_parse)

def parse_xml_1(file_name):
    # 1st version of iterative parser based on standart xml lib 
    events = ("start", "end")
    context = ET.iterparse(file_name, events=events)
    items_list = recur(context)['DataFeeds']['item_data']
    # items_list = []
    write_to_mongo(items_list)
    print '{} item(s) parsed from {}'.format(len(items_list), file_to_parse)

def create_db():
    client = MongoClient(port=27017)
    db = client[db_name]
    return db

def write_to_mongo(data):
    # create mongo db and insert all data
    db = create_db()
    f = db.items.insert_many(data)
    print '{} item(s) inserted into {}'.format(len(f.inserted_ids), db_name)

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

if __name__ == "__main__":

    if file_to_parse.split('.')[1] in ['txt', 'csv']: 
        parse_csv_1(file_to_parse)

    elif file_to_parse.split('.')[1] in ['xml']:

        func = globals()['parse_xml_{}'.format(xml_parser_version)]
        func(file_to_parse)
