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

        cur = self.db_citygis.cursor()

        # Haal alle unieke datums op
        # TODO: Optimaliseren, misschien alleen van laatste twee dagen? Afhankelijk van vertraging ontvangst gegevens
        cur.execute("SELECT DISTINCT unit_id, DATE_FORMAT(`datetime`, '%Y-%m-%d') FROM connection ORDER BY DATE(`datetime`) DESC")

        dagen = list(cur.fetchall())
        for dag in dagen:
            #Haal voor deze combinatie van dag en unit alle connections op, zowel succesvol als failed
            cur = self.db_citygis.cursor()
            sql = ("select (SELECT COUNT(*) FROM connection WHERE unit_id = " + str(dag[0]) + " AND DATE_FORMAT(`datetime`, '%Y-%m-%d') = '" + dag[1] + "'  AND `value` = 0) AS failed,"
            "(SELECT COUNT(*) FROM connection WHERE unit_id = " + str(dag[0]) + " AND DATE_FORMAT(`datetime`, '%Y-%m-%d') = '" + dag[1] + "' AND `value` = 1) AS succes")
            cur.execute(sql)
            row = list(cur.fetchone())

            self.__insertAggregateConnection(dag[0], dag[1], row[0], row[1])




    def __insertAggregateConnection(self, unit_id, date, success, failed):
        #Voor nu: insert waardes in mysql database, totdat webservice klaar is
        #add_aggreg_conn = ("INSERT INTO `connection` (unit_id, `date`, succesful, failed) VALUES (%s, %s, %s, %s)")
        #data_aggreg_conn = (unit_id, date, success, failed)

        #cur = self.db_aggregate.cursor()
        #cur.execute(add_aggreg_conn, data_aggreg_conn)

        #self.db_aggregate.commit()
        #print cur.lastrowid
        #succesvolle connections
        #data = demjson.encode(['meting_type': 'CS','voertuig_id': null ,'meting_datum':date, 'waarde': success, 'unit_id': unit_id])

        url = "http://145.24.222.120/citygis/meting"
        succesJson =  { 'meting_type' : 'CS', 'voertuig_id' : unit_id, 'meting_datum' : date, 'waarde' : success, 'unit_id' : -1 }
        failedJson =  { 'meting_type' : 'CF', 'voertuig_id' : unit_id, 'meting_datum' : date, 'waarde' : failed, 'unit_id' : -1 }
        print 'input: ' ,succesJson

        req = urllib2.Request(url, json.dumps(succesJson), headers={'Content-type': 'application/json', 'Accept': 'application/json'})
        response = urllib2.urlopen(req)
        the_page = response.read()
        print 'Output: ', the_page



aggregate = CityGisAggregator()
aggregate.aggregateConnection()
