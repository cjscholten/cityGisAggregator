#!/usr/bin/env python

import MySQLdb
import json
import urllib
import urllib2

class CityGisAggregator:

    def __init__(self):
        self.db_citygis = MySQLdb.connect(host="localhost",
             user="root",
             passwd="Z32hKL",
             db="citygis")

        self.db_aggregate = MySQLdb.connect(host="localhost",
             user="root",
             passwd="Z32hKL",
             db="aggregate_results")


    def aggregateConnection(self):
        
        #Haal alle waarden op voor foute connectie
        cur = self.db_citigis.cursor();        
        cur.execute("select unit_id, DATE_FORMAT(`datetime`, '%Y-%m-%d'), count(*) from connection where `value` = 0 group by unit_id, DATE_FORMAT(`datetime`, '%Y-%m-%d')")
        failed = list(cur.fetchall())
        for row in failed:
            self.__insertMeting(row[0],row[1],row[2],'CF')
            
            
        #Haal alle waarden op voor goede connectie
        cur = self.db_citigis.cursor();        
        cur.execute("select unit_id, DATE_FORMAT(`datetime`, '%Y-%m-%d'), count(*) from connection where `value` = 1 group by unit_id, DATE_FORMAT(`datetime`, '%Y-%m-%d')")
        success = list(cur.fetchall())
        for row in success:
            self.__insertMeting(row[0],row[1],row[2],'CS')
                




    def __insertMeting(self, unit_id, date, waarde, metingTYpe):
        url = "http://145.24.222.120/citygis/meting"
        insertJson =  { 'meting_type' : metingType, 'voertuig_id' : unit_id, 'meting_datum' : date, 'waarde' : waarde }
        
        req = urllib2.Request(url, json.dumps(insertJson), headers={'Content-type': 'application/json', 'Accept': 'application/json'})
        response = urllib2.urlopen(req)
        echo response.read()

     


aggregate = CityGisAggregator()
aggregate.aggregateConnection()
