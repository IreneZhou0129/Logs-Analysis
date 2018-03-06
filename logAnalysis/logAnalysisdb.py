
import psycopg2

# go to the correct directory,
# >> vagrant up
# >> vagrant ssh
# To load the data, cd into the vagrant directory
# >> cd /vagrant
# first time use 
# >>vagrant@vagrant:/vagrant/logAnalysis$ psql -d news -f newsdata.sql
# next time when I want to open the database
#...psql -d news
# but in this project I run logAnalysis.py file
#vagrant@vagrant:/vagrant/logAnalysis$ python logAnalysis.py

def get_top_articles():
    
    DBNAME = "news"
    db = psycopg2.connect(database = DBNAME)
    c = db.cursor()
    
    # the result table has two columns: title and Views number
    c.execute("SELECT title, COUNT(*) FROM "
              "articles INNER JOIN log ON "
              #use SUBSTRING to take the article title from column PATH
              "SUBSTRING(log.path,10) = articles.slug " 
              #GROUP BY used with COUNT to group the articles
              # with same title together followed by
              # their apperance times
              "GROUP BY articles.title ORDER BY COUNT DESC;")    


    # entities are listed in the rows
    rows = c.fetchall()
    for row in rows:
        # row[0] represents the Title column
        # row[1] represents the Views column
        print row[0]," ",row[1]," views"
    # close the database     
    db.close()


def get_top_authors():


    DBNAME = "news"
    db = psycopg2.connect(database = DBNAME)
    c = db.cursor()


    # the result table has columns Author Name, and Article Views
    c.execute("SELECT name, COUNT(*) FROM "
              # table authors INNER JOIN with the 
              # result of second INNER JOIN
              "(authors INNER JOIN "
                # the second INNER JOIN create a table 
                # with all columns and entities two table have
                "(articles INNER JOIN "
                "log ON SUBSTRING(log.path,10)= articles.slug) "
              "ON authors.id = articles.author) "
              "GROUP BY authors.name ORDER BY COUNT DESC;"
                )


    rows = c.fetchall()
    for row in rows:
        print row[0], " ",row[1]
    db.close()



def get_top_days():

    DBNAME = "news"
    db = psycopg2.connect(database = DBNAME)
    c = db.cursor()

    c.execute(
        # divide the two columns and get the error rate in % 
        "SELECT aa.date, err/total*100 "

        # the following queries create a table with three columns:
        # date | total | err
        
        "FROM "
        
            # return the table with two columns: 
            # date | total
            # the total column returns the amount of occurance for 
            # both 404 not found status and OK status
            "(SELECT a.date,SUM(count) as total "
            "FROM " 
            # return the table with two columns:
            # status | date | count
            # there are 62 rows, which means the column COUNT counts 
            # total occurance for each status in each day
                "(SELECT status, DATE(time),count(status) "
                "FROM log "
                "GROUP BY status, DATE(time) "
                ") AS a "
            "GROUP BY a.date) as aa"

        "INNER JOIN "


            # return the table with two columns: 
            # date | err
            # the ERR column returns the total amount of occurance for 
            # 404 not found status on each day 
            "(SELECT b.date,SUM(count) as err "
            "FROM "
                "(SELECT status, DATE(time),COUNT(status)  "
                "FROM log  "
                "WHERE status = '404 NOT FOUND' "
                "GROUP BY status, DATE(time) "
                ")AS b "
            "GROUP BY b.date) as bb "

        "ON (aa.date = bb.date) "
        

        # the question only wants the result 
        # shows the day with more than 1% error 
        " WHERE err/total*100 > 1 "
        " GROUP BY aa.date , err/total*100 "
        " ORDER BY aa.date; "   
        )

    rows = c.fetchall()
    for row in rows:
        print row[0], " ",row[1]
    db.close()
