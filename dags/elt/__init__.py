import datetime
import json
import uuid
import psycopg2
import psycopg2.extras


class DbConnection(object):
    """
    Class to allow for interacting with psycopg2, reusing psycopg2 heavyweight connection,
    managing transaction, ... etc.
    """
    def __init__(self, connection, readonly=False, autocommit=False):
        self.connection = psycopg2.connect(**connection)
        self.connection.set_session(readonly=readonly, autocommit=autocommit)


    def execute_inTransaction(self, sql, params=None):
        """
        Execute sql statement as a single transaction
        :return rowcount impacted
        """
        # connection context manager: if no exception raised in Context, then committed (otherwise rolled back)
        with self.connection as c:
            # cursor Context manager : close/release any resource held by cursor (ex. result cache)
            with c.cursor() as curs:
                curs.execute(sql, params)
                return curs.rowcount

    def execute(self, sql, params=None):
        """
        Execute sql statement while leaving open the transaction.
        :return rowcount impacted
        """
        with self.connection.cursor() as curs:
            curs.execute(sql, params)
            return curs.rowcount

    def copy_into_table(self, schematable, columns, open_file, delim='|'):
        """
        Execute copy_expert
        :return rowcount impacted
        """
        sql = \
            """
            copy %s( %s )
            from STDIN with csv HEADER DELIMITER '%s' NULL '';
            """ % (schematable, columns, delim)

        with self.connection.cursor() as curs:
            curs.copy_expert(sql, open_file, size=8192)
            return curs.rowcount

    def fetch_one(self, sql, params=None):
        """
        Execute sql query and fetch one row
        :param sql:
        :param params:
        :return: fetched row
        """
        with self.connection.cursor() as curs:
            curs.execute(sql, params)
            one_row = curs.fetchone()
        return one_row

    def fetch_all(self, query, params=None, in_trans=False, as_dict=False):
        """
        Execute query, return all records as a list of tuple (or as dictionary)
        leaving open the transaction if in_trans=False or commit otherwise.
        """
        if as_dict:
            cur = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur = self.connection.cursor()
        cur.execute(query, params)
        result = cur.fetchall()
        cur.close()
        if in_trans:
            self.connection.commit()
        return result

    def insert_row_get_id(insert, params=None):
        """
        Insert a single row while leaving open the transaction.
        :return: the auto-generated id
        """
        if insert.rfind(";") == -1:
            insert += ' RETURNING id;'
        else:
            insert = insert.replace(';', ' RETURNING id;')

        with self.connection.cursor() as curs:
            curs.execute(insert, params)
            return curs.fetchone()[0]


    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def __del__(self):
        self.connection.close()

    def __str__(self):
        return self.connection.__str__()


# Singleton Dbconnection on default database
conn_readonly = None

def get_ro_connection():
    global conn_readonly
    if conn_readonly is None:
        # autocommit=True to be sure no transaction started (even for select)
        conn_readonly = DbConnection(connection=config.DATABASE, readonly=True, autocommit=True)
    return conn_readonly

conn = None

def get_connection():
    global conn
    if conn is None:
        conn = DbConnection(connection=config.DATABASE)
    return conn


