#
# Assignment2 Interface
#

import psycopg2
import os
import sys
from threading import Thread

# Do not close the connection inside this file i.e. do not perform openConnection.close()
def fragments (openConnection):
    cur = openConnection.cursor()

    cur.execute('Select max(latitude2 - latitude1) from rectangles')

    longestRectH = cur.fetchone()[0]
    print("longestRectH",longestRectH)

    cur.execute('select max(latitude2) from rectangles') 
    
    
    greatestLat_raw = cur.fetchone()
    greatestLat = float(''.join(map(str, greatestLat_raw))) 
    print("greatestLat",greatestLat)

    
    cur.execute('select min(latitude1) from rectangles') 
    smallestLat_raw = cur.fetchone()
    smallestLat = float(''.join(map(str, smallestLat_raw))) 
    print("smallestLat",smallestLat)

    fullLength=greatestLat - smallestLat  
    
    lengthFrag = (greatestLat - smallestLat) / 4  
    
    fragcHeight=longestRectH/2  

    cur.execute('DROP TABLE IF EXISTS fragment_p1') #If the table already exists drop them, frag_p1 and so on, are the 4 fragments each created for points and rectangle
    cur.execute('DROP TABLE IF EXISTS fragment_p2')
    cur.execute('DROP TABLE IF EXISTS fragment_p3')
    cur.execute('DROP TABLE IF EXISTS fragment_p4')
    cur.execute('DROP TABLE IF EXISTS fragment_r1')
    cur.execute('DROP TABLE IF EXISTS fragment_r2')
    cur.execute('DROP TABLE IF EXISTS fragment_r3')
    cur.execute('DROP TABLE IF EXISTS fragment_r4')


    
    cur.execute(
            'CREATE TABLE fragment_r1 AS select * from rectangles where latitude2 <' + str(smallestLat + lengthFrag+fragcHeight))
    cur.execute(
            'CREATE TABLE fragment_r2 AS select * from rectangles where latitude1 >= ' + str(smallestLat + lengthFrag-fragcHeight)
            + ' and ' + ' latitude2 <' + str(smallestLat + (2*lengthFrag) +fragcHeight))  # 2 times fragmentation point indicates 50% of the length covered and similarly 3 times indicates 75% of the length is covered.
    cur.execute(
            'CREATE TABLE fragment_r3 AS select * from rectangles where latitude1 >= ' + str(smallestLat + (2*lengthFrag) -fragcHeight)
            + ' and ' + ' latitude2 <' + str(smallestLat + (3*lengthFrag) +fragcHeight))
    cur.execute(
            'CREATE TABLE fragment_r4 AS select * from rectangles where latitude1 >= ' + str(smallestLat + (3*lengthFrag) -fragcHeight)
            + ' and ' + ' latitude2 <=' + str(smallestLat + fullLength + fragcHeight))



    cur.execute(
            'CREATE TABLE fragment_p1 AS select * from points where latitude <' + str(smallestLat + lengthFrag + fragcHeight))
    cur.execute(
            'CREATE TABLE fragment_p2 AS select * from points where latitude >= ' + str(smallestLat + lengthFrag - fragcHeight)
            + ' and ' + ' latitude < ' + str(smallestLat + (2*lengthFrag) + fragcHeight))
    cur.execute(
            'CREATE TABLE fragment_p3 AS select * from points where latitude >= ' + str(smallestLat + (2*lengthFrag) - fragcHeight)
            + ' and ' + ' latitude < ' + str(smallestLat + (3*lengthFrag) + fragcHeight))
    cur.execute(
            'CREATE TABLE fragment_p4 AS select * from points where latitude >= ' + str(smallestLat + (3*lengthFrag) - fragcHeight)
            + ' and ' + ' latitude < ' + str(smallestLat + fullLength + fragcHeight)
            )
    cur.close()
    openConnection.commit()



def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    cur = openConnection.cursor()
    fragments(openConnection) #outputtable
    cur.execute('DROP TABLE IF EXISTS' + outputTable)
    cur.execute("CREATE TABLE" + outputTable + " (points_count bigint, rectgeo geometry)")

    t1 = Thread(target = joinFragments, args =('fragment_p1','fragment_r1',cur,outputTable))
    t2 = Thread(target = joinFragments, args =('fragment_p2','fragment_r2',cur,outputTable))
    t3 = Thread(target = joinFragments, args =('fragment_p3','fragment_r3',cur,outputTable))
    t4 = Thread(target = joinFragments, args =('fragment_p4','fragment_r4',cur,outputTable))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    cur.execute("SELECT DISTINCT points_count,rectgeo from "+ outputTable+" order by points_count asc")
    tuples = cur.fetchall()
    file = open(outputPath,"a")
    for tuple in tuples:
        file.write(str(tuple[0])+'\n')
    file.close()
    print('Join is successfully executed')
    openConnection.commit()


    def joinFragments(pointsTable,rectsTable,cur,outputTable):
        cur.execute('INSERT INTO ' + outputTable + '(points_count,rectgeo) SELECT count(' + pointsTable + '.geom) AS count, ' + rectsTable
                   + '.geom as rectgeo FROM ' + rectsTable
                   + 'JOIN ' + pointsTable + ' ON st_contains('+ rectsTable + '.geom,' + pointsTable+ '.geom) GROUP BY'
                   + rectsTable + '.geom order by count asc')



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


