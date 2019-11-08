import partitioning as Partioning
import rangequery as RangeQuery

print("Creating Database Name dds")
Partioning.createDB()

print("Getting connection from dds database")
con = Partioning.getOpenConnection()

print("Creating and Loading the ratings table")
Partioning.loadRatings('ratings','test_data.dat',con)

print("Doing Range Partitioning")
Partioning.rangePartition('ratings',5,con)

print("Deleting the Main Table")
Partioning.deleteTables('ratings',con)

print("Performing Range Query")
RangeQuery.RangeQuery('ratings',1.5,3.5,con)

print("Deleting all Tables")
Partioning.deleteTables('all',con)

if(con):
    con.close()

# https://github.com/aditya-chayapathy/distributed-database-systems/tree/master/Assignments
