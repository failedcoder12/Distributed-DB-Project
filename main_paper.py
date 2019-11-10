import partitioning_paper as Partioning
import rangequery_paper as RangeQuery

print("Creating Database Name dds")
Partioning.createDB()

print("Getting connection from dds database")
con = Partioning.getOpenConnection()

print("Creating and Loading the ratings table")
Partioning.loadRatings('ratings','data/IJdata.dat',con)

print("Doing Range Partitioning")
Partioning.rangePartition('ratings',53,con)

print("Performing Range Query")
RangeQuery.RangeQuery('ratings','aa','az',con)


print("Deleting all Tables")
Partioning.deleteTables('all',con)
