import pytest
from pymongo import MongoClient 
from parser import parse_csv, parse_xml_1, parse_xml_2

class TestMongo:
    def setup(self):
        self.client = MongoClient(port=27017)
        self.db = self.client['pytest_db']

    def teardown(self):
        self.client.drop_database('pytest_db')

    def test_parse_xml_1(self):
        parse_xml_1('small_xml.xml')
        
        found = self.db.xml_items.find({'id': "B00VINDBJK" }).count()
        assert found == 1

    def test_parse_xml_2(self):
        parse_xml_2('small_xml.xml')
        
        found = self.db.xml_items.find({'id': "B00VINDBJK" }).count()
        assert found == 1

    def test_parse_csv(self):
        parse_csv('small_csv.txt')
        found = self.db.csv_items.find({'id': 402983319863 }).count()
        assert found == 1