#!/usr/bin/env python

import MySQLdb
import json
import urllib
import urllib2
from geopy.distance import vincenty

class CityGisAggregator:

    def __init__(self):
        self.db_citygis = MySQLdb.connect(host="localhost",
             user="root",
             passwd="Z32hKL",
             db="citygis")


    def aggregateConnection(self):

        cur = self.db_citygis.cursor()

        #Haal alle waarden op voor foute connectie
        cur.execute("select unit_id, DATE_FORMAT(`datetime`, '%Y-%m-%d'), count(*) from connection where `value` = 0 group by unit_id, DATE_FORMAT(`datetime`, '%Y-%m-%d')")
        failed = list(cur.fetchall())
        for row in failed:
            self.__insertMeting(row[0],row[1],row[2],'CF')

     
        #Haal alle waarden op voor goede connectie
        cur.execute("select unit_id, DATE_FORMAT(`datetime`, '%Y-%m-%d'), count(*) from connection where `value` = 1 group by unit_id, DATE_FORMAT(`datetime`, '%Y-%m-%d')")
        success = list(cur.fetchall())
        for row in success:
            self.__insertMeting(row[0],row[1],row[2],'CS')



    def aggregateDistance(self):

        cur = self.db_citygis.cursor()  
        #Ophalen alle unieke combinaties van voertuig per dag. 
        cur.execute("select distinct unit_id, DATE_FORMAT(`datetime`, '%Y-%m-%d') from position")
        days = list(cur.fetchall())
        #loopen door alle records om per voertuig per dag alle position records op te halen.        
        for day in days:
            totalDistance = 0
            cur = self.db_citygis.cursor()
            sql = "select rdx, rdy from position where unit_id = " + str(day[0]) + " and DATE_FORMAT(`datetime`, '%Y-%m-%d') = '" + day[1] + "'"            
            cur.execute(sql)
            positions = list(cur.fetchall())
            posOud = [0,0]            
            posNew = [0,0]
            
            #per voertuig per dag door de position records loopen om de afstand tussen alle records te bepalen, en zo de totale afstand te berekenen
            for pos in positions:
                posNew = pos 
                
                if (posOud[0] != 0 and posOud[1] != 0):
                    distance = vincenty(posOud, posNew).meters
                    totalDistance += distance 
                    
                posOud = posNew 
                
                       
            self.__insertMeting(day[0],day[1],totalDistance,'PS')
           
            


    def __insertMeting(self, unit_id, date, waarde, metingType):
        url = "http://145.24.222.120/citygis/meting"
        insertJson =  { 'meting_type' : metingType, 'voertuig_id' : unit_id, 'meting_datum' : date, 'waarde' : waarde }
        #print insertJson
        
        req = urllib2.Request(url, json.dumps(insertJson), headers={'Content-type': 'application/json', 'Accept': 'application/json'})
        response = urllib2.urlopen(req)
        print response.read()


        

aggregate = CityGisAggregator()
#aggregate.aggregateConnection()
aggregate.aggregateDistance()
