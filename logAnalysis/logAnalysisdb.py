import psycopg2

#list from the most viewed articles to the least viewed article 
def get_top_articles():
    #To load the data, cd into the vagrant directory
    # like this, cd /vagrant
    # and use the command, psql -d news -f newsdata.sql
    DBNAME = "news"
    db = psycopg2.connect(database = DBNAME)
    c = db.cursor()
            #use SUBSTRING to take the article title from column PATH
    c.execute("SELECT SUBSTRING(path,10), COUNT(*) count "
              "FROM log " 
              "GROUP BY path "
    #use '200' to filter out unexpected results,eg.,'so-many-bearsk 30 views'
    #in these cases, the article titles are always appended by one letter of the alphabet
              "HAVING COUNT(*)>200 "
              "ORDER BY count DESC; ")         
    rows = c.fetchall()
    for row in rows:
        print row[0]," ",row[1]," views \n"
    db.close()


def get_top_authors():

    DBNAME = "news"
    db = psycopg2.connect(database = DBNAME)
    c = db.cursor()
    c.execute(
                  )
    rows = c.fetchall()
    for row in rows:
        print row[0]
    db.close()