import psycopg2


def run():
    """ Connect to the PostgreSQL database server """
    sql1 = """INSERT INTO fly_booking(client_name, fly_number, "from", "to", date)
             VALUES(%s, %s, %s, %s, %s)"""
    sql2 = """INSERT INTO hotel_booking(client_name, hotel_name, arrival, departue)
             VALUES(%s, %s, %s, %s)"""
    sql3 = """UPDATE account 
              SET amount = amount - %s
              WHERE client_name = %s"""

    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL DB1...')
        connDB1 = psycopg2.connect(dbname="DB1", user="postgres", password="1")
        print('Connecting to the PostgreSQL DB2...')
        connDB2 = psycopg2.connect(dbname="DB2", user="postgres", password="1")
        print('Connecting to the PostgreSQL DB3...')
        connDB3 = psycopg2.connect(dbname="DB3", user="postgres", password="1")
        # connDB1.tpc_begin(connDB1.xid(42, 'transaction ID', 'connection 1'))
        # connDB2.tpc_begin(connDB2.xid(42, 'transaction ID', 'connection 2'))
        xid1 = connDB1.xid(1, 'gtrid', 'bqual')
        xid2 = connDB2.xid(2, 'gtrid', 'bqual')
        xid3 = connDB3.xid(3, 'gtrid', 'bqual')

        connDB1.tpc_begin(xid1)
        connDB2.tpc_begin(xid2)
        connDB3.tpc_begin(xid3)

        cur1 = connDB1.cursor()
        cur2 = connDB2.cursor()
        cur3 = connDB3.cursor()

        # execute the INSERT statement
        cur1.execute(sql1, ('Hadi Busby', 'PS 5411', 'Kyiv', 'Kayseri', '2022-03-11'))
        cur2.execute(sql2, ('Hadi Busby', 'Hilton', '2022-03-11', '2022-03-12'))
        cur3.execute(sql3, (str(10), 'Hadi Busby'))
        try:
            connDB1.tpc_prepare()
            connDB2.tpc_prepare()
            connDB3.tpc_prepare()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            connDB1.tpc_rollback()
            connDB2.tpc_rollback()
            connDB3.tpc_rollback()
        else:
            connDB1.tpc_commit()
            connDB2.tpc_commit()
            connDB3.tpc_commit()
            print('Success commit')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        # if conn is not None:
        connDB1.close()
        connDB2.close()
        connDB3.close()
        print('Database connection closed.')
