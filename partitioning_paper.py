import psycopg2
import os
import sys

PRIME = 53
DATABASE_NAME = 'dds'

def hash_string(id):
    num = 0
    for i in id:
        num = (num*PRIME + ord(i))%PRIME
    return num


def getOpenConnection(user='postgres',password='1234',dbname='dds'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def createDB(dbname='dds'):

    #Connect to default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    #Check whether database exists or not
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' %(dbname,))
    count = cur.fetchone()[0]

    if(count == 0):
        cur.execute('CREATE DATABASE %s' %(dbname,))
    else:
        print('A database named {0} already exists'.format(dbname))

    cur.close()
    con.commit()
    con.close()

def loadRatings(rantingstablename, ratingfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS " + rantingstablename)

    cur.execute("CREATE TABLE " + rantingstablename + " (UserID VARCHAR(1001), Rating INT)")

    loadout = open(ratingfilepath,'r')

    cur.copy_from(loadout,rantingstablename,sep=',',columns=('UserID','Rating'))
  
    cur.close()
    openconnection.commit()

def rangePartition(rantingstablename, numberofpartitions, openconnection):
    name = "RangeRatingsPart"

    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" %rantingstablename)

        if(not bool(cursor.rowcount)):
            print("Please Load Ratings Table first!!!")
            return
        
        cursor.execute("CREATE TABLE IF NOT EXISTS RangeRatingsMetadata(PartitionNum INT, HASHRating INT)")

        i = 0

        while(i<numberofpartitions):
            newTableName = name + `i`
            cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID VARCHAR(1001), Rating INT)" %(newTableName))
            i = i + 1
        
        i = 0

        while(i < PRIME):
            cursor.execute("SELECT * FROM %s" %(rantingstablename))
            rows = cursor.fetchall()
            
            newTableName = name + `i`
            for row in rows:
                hash_val = hash_string(row[0])
                if(hash_val == i):
                    cursor.execute("INSERT INTO " + newTableName + " (UserID, Rating) VALUES('" + row[0] + "','" + str(row[1])+"')")
            
            cursor.execute("INSERT INTO RangeRatingsMetadata (PartitionNum, HASHRating) VALUES(%d,%d)" %(i,i))
            i = i+1
        
        openconnection.commit()

    except psycopg2.DatabaseError, e:
        if(openconnection):
            openconnection.rollback()
        print('Error %s' %e)
        sys.exit(1)
    except IOError, e:
        if(openconnection):
            openconnection.rollback()
        print('Error %s' %e)
        sys.exit(1)
    finally:
        if(cursor):
            cursor.close()

def deleteTables(rantingstablename, openconnection):
    try:
        cursor = openconnection.cursor()

        if(rantingstablename.upper() == 'ALL'):
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()

            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' %(table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' %(rantingstablename))
        
        openconnection.commit()
    
    except psycopg2.DatabaseError, e:
        if(openconnection):
            openconnection.rollback()
        print('Error %s' %e)
        sys.exit(1)
    except IOError, e:
        if(openconnection):
            openconnection.rollback()
        print('Error %s' %e)
        sys.exit(1)
    finally:
        if(cursor):
            cursor.close()