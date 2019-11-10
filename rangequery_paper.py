import psycopg2
import os
import sys

PRIME = 53
def hash_string(id):
    num = 0
    for i in id:
        num = (num*PRIME + ord(i))%PRIME
    return num

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, keylength, openconnection):
    data = []
    
    hash_got = {}
    list_req = [chr(c+ord('a')) for c in range(0,26)]    
    heapq.heapify(list_req)
    
    while(len(list_req) > 0):
        st = heapq.heappop(list_req)
        
        if(len(st) < keylength):
            hash_got[hash_string(st)] = 1
            for c in range(0,26):
                st = st + chr(c + ord('a'))
                heapq.heappush(list_req,st)
        
        if(len(hash_got) >= 26):
            break
    
    cur = openconnection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchall()[0][0]

    cur.execute('select table_name from information_schema.tables where table_name like \'%rangeratingspart%\' and table_catalog=\'' + db_name + '\'')
    range_partitions = cur.fetchall()

    for table in range_partitions:
        table_name = table[0]
        cur.execute("select userid, rating from " + str(table_name) + " where userid >= '" + ratingMinValue + "' and userid <= '" + ratingMaxValue + "'")
        matches = cur.fetchall()

        for match in matches:
            partition = "RangeRatingsPart" + table_name[-1]
            data.append(str(partition) + "," + str(match[0]) + "," + str(match[1]))
    
    cur.close()

    fh = open("RangeQueryOut.txt","w")
    fh.write("\n".join(data))
    fh.close()

import heapq

def FastRangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, keylength, openconnection):
    data = []
    cur = openconnection.cursor()

    hash_got = {}
    list_req = [chr(c+ord('a')) for c in range(0,26)]    
    heapq.heapify(list_req)
    
    while(len(list_req) > 0):
        st = heapq.heappop(list_req)
        
        if(len(st) < keylength):
            hash_got[hash_string(st)] = 1
            for c in range(0,26):
                st = st + chr(c + ord('a'))
                heapq.heappush(list_req,st)
        
        if(len(hash_got) >= 26):
            break
        
    
    for table in hash_got:
        table_name = name = "RangeRatingsPart" + str(table)
        cur.execute("select userid, rating from " + table_name + " where userid >= '" + ratingMinValue + "' and userid <= '" + ratingMaxValue + "'")
        matches = cur.fetchall()

        for match in matches:
            partition = table_name
            data.append(str(partition) + "," + str(match[0]) + "," + str(match[1]))
    
    cur = openconnection.cursor()

    cur.close()
    fh = open("RangeQueryOut.txt","w")
    fh.write("\n".join(data))
    fh.close()
