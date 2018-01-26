
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
