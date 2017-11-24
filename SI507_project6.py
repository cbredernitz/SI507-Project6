
import psycopg2
import psycopg2.extras
import sys
import csv

##### Setting up the database connection #####
try:
    conn = psycopg2.connect("dbname = 'brederni_507project6' user = 'Chris'")
    print("Success connecting to the database")

except:
    print("Unable to connect to the database")
    sys.exit(1)

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

##### Seting up the database #####
def setup_database():
    cur.execute("""CREATE TABLE IF NOT EXISTS States(
                ID SERIAL PRIMARY KEY NOT NULL,
                Name VARCHAR(128) UNIQUE NOT NULL
                )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS Sites(
                ID SERIAL NOT NULL,
                Name VARCHAR(128) UNIQUE NOT NULL,
                Type VARCHAR(128),
                State_ID INTEGER REFERENCES States(ID),
                Location VARCHAR(225),
                Description TEXT)""")

    # cur.execute('DROP TABLE SITES')
    # cur.execute('DROP TABLE STATES')

    conn.commit()

##### Writing the data from the csv files into the database #####
#  Setting up State Table
def write_database():

# Arkansas data
    with open('arkansas.csv', 'r') as ak_csv:
        ak = csv.DictReader(ak_csv)
        cur.execute("""INSERT INTO States(Name) VALUES('Arkansas') RETURNING ID""")
        result = cur.fetchone()
        for row in ak:
            row['STATE_ID'] = result[0]
            cur.execute("""INSERT INTO Sites(Name, Type, State_ID, Location, Description) VALUES(%(NAME)s, %(TYPE)s, %(STATE_ID)s, %(LOCATION)s, %(DESCRIPTION)s) on conflict do nothing""", row)

# California data
    with open('california.csv', 'r') as cal_csv:
        cal = csv.DictReader(cal_csv)
        cur.execute("""INSERT INTO States(Name) VALUES('California') RETURNING ID""")
        result = cur.fetchone()
        for row in cal:
            row['STATE_ID'] = result[0]
            cur.execute("""INSERT INTO Sites(Name, Type, State_ID, Location, Description) VALUES(%(NAME)s, %(TYPE)s, %(STATE_ID)s, %(LOCATION)s, %(DESCRIPTION)s) on conflict do nothing""", row)

# Michigan data
    with open('michigan.csv', 'r') as mi_csv:
        mi = csv.DictReader(mi_csv)
        cur.execute("""INSERT INTO States(Name) VALUES('Michigan') RETURNING ID""")
        result = cur.fetchone()
        for row in mi:
            row['STATE_ID'] = result[0]
            cur.execute("""INSERT INTO Sites(Name, Type, State_ID, Location, Description) VALUES(%(NAME)s, %(TYPE)s, %(STATE_ID)s, %(LOCATION)s, %(DESCRIPTION)s) on conflict do nothing""", row)

    conn.commit()

# Write code to be invoked here (e.g. invoking any functions you wrote above)

def execute_and_print(query, numer_of_results=1):
    cur.execute(query)
    results = cur.fetchall()
    for r in results[:numer_of_results]:
        print(r)
    print('--> Result Rows:', len(results))

def run_query():
    print('==> Getting all locations')
    all_locations = execute_and_print('SELECT Location FROM Sites')
    print('\n')

    print('==> Getting all beautiful sites')
    beautiful_sites = execute_and_print("""SELECT Name FROM Sites WHERE Description ILIKE '%beautiful%'""")
    print('\n')

    print('==> Getting all the National Lakeshore Parks')
    natl_lakeshores = execute_and_print("""SELECT count(Type) from Sites WHERE Type = 'National Lakeshore'""")
    print('\n')

    print('==> Getting all National Parks in Michigan')
    michigan_names = execute_and_print("""SELECT States.Name AS State_name, Sites.Name AS Site_name FROM Sites INNER JOIN States ON Sites.State_ID = States.ID WHERE States.Name LIKE '%Michigan%'""")
    print('\n')

    print('==> Getting a count of National Parks in Arkansas')
    total_number_arkansas = execute_and_print("""SELECT count(Sites.Name) FROM Sites INNER JOIN States ON Sites.State_ID = States.ID WHERE States.Name LIKE '%Arkansas%'""")
    print('\n')

if __name__ == '__main__':
    command = None
    if len(sys.argv) > 1:
        command = sys.argv[1]
    if command == 'setup':
        setup_database()
        print('-- Setting up database --')
    elif command == 'write':
        write_database()
        print('-- Writting database --')
    else:
        print('-- Running Queries --')
        run_query()

# We have not provided any tests, but you could write your own in this file or another file, if you want.
