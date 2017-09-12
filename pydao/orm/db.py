#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on 28/03/2014

@author: thiago-amm
'''

__author__ = 'Thiago Alexandre Martins Monteiro'
__date__ = '24/11/2012'

class Connection(object):
    '''
    Class:       Connection
    Module:      pydao.db
    Description: Represent connections to the database.
    '''
    def __init__(self, driver_name=None, dbms=None, host=None, port=None,
                 user=None, password=None, database=None, auto_increment=False,
                 auto_commit=False
    ):
        '''
        Method:             __init__
        Description:        Constructor that initializes objects of this class. 
                            NOTE: it is a magic method.
        Parameters:
            self:           The current object reference (memory address).
            driver_name:    The name of the database module (default None).
            dbms:           The name of the DBMS (DATABASE MANAGEMENT SYSTEM - database server) used. 
                            Example: mysql (default None).
            host:           The host name (default None).
            port:           The port in the host where the DBMS server process will listen for connections (default None).
            user:           The database user (default None).
            password:       The database user password (default None).
            auto_increment: Indicates whether the auto increment feature must be enabled for key numeric field (default False).
            auto_commit:    Indicates whether the auto commit feature must be enabled for this connection (default False).
        Return:             none.
        Usage:              conn = Connection(
                                drive_name='MySQLdb', dbms='mysql', host='localhost', port='3306', user='root', password='root',
                                database='mydatabase', auto_increment=True, auto_commit=True
                            )
        '''
        self.__driver_name = driver_name
        self.__dbms = dbms
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__database = database
        self.__auto_increment = auto_increment
        self.__auto_commit = auto_commit

        try:
            if driver_name:
                # Importing module dinamically
                self.__driver = __import__(driver_name)
        except Exception, e:
            print e.message
        self.__reference = None
        self.__cursor = None

    def __del__(self):
        '''
        Method:      __del__
        Description: Desctructor that perform operations before destroy (deallocate memory) objects of this class.
                     NOTE: it is a magic method.
        Parameters:
            self:    The current object reference.
        Return:      none.
        Usage:       del conn
        '''
        if self.__cursor:
            self.__cursor.close()
            self.__reference.close()

    @property
    def driver_name(self):
        '''
        Property:    driver_name
        Description: Returns the driver name (Python module) used to create this connection with a database.
        Parameters:
            self:    The current object reference.
        Return:      The driver name value (a string).
        Usage:       driver = conn.driver_name
        '''
        return self.__driver_name

    @property
    def dbms(self):
        '''
        Property:    dbms
        Description: Returns the DBMS (DATABASE MANAGEMENT SYSTEM - database server) name where the connection will be done.
        Parameters:
            self:    The current object reference.
        Return:      The DBMS name value (string).
        Usage:       dbms = conn.dbms
        '''
        return self.__dbms

    @property
    def host(self):
        '''
        Property:    host
        Description: Returns the host name or IP address of the computer that contains the database server.
        Parameters:
            self:    The current object reference.
        Return:      The host name value (string).
        Usage:       host = conn.host
        '''
        return self.__host

    @property
    def port(self):
        '''
        Property:    port   
        Description: Returns the port used in the connection.
        Parameters:
            self:    The current object reference.
        Return:      The port value (string).
        Usage:       port = conn.port
        '''
        return self.__port

    @property
    def user(self):
        '''
        Property:    user
        Description: Returns the name of the database user.
        Parameters:
            self:    The current object reference.
        Return:      The database user name (string).
        Usage:       user = conn.user
        '''
        return self.__user

    @property
    def password(self):
        '''
        Property:    password
        Description: Returns database user password.
        Parameters:
            self:    The current object reference.
        Return:
                     The database user password (string).
        Usage:       password = conn.password
        '''
        return self.__password

    @property
    def database(self):
        '''
        Property:    database
        Description: Returns the name of the database that will be used.
        Parameters:
            self:    The current object reference.
        Return:      The name of the database (string).
        Usage:       database = conn.database
        '''
        return self.__database

    @property
    def driver(self):
        '''
        Property:    driver
        Description: Returns a reference to the driver module used by the connection.
        Parameters:
            self:    The current object reference.
        Return:      The reference (memory address) to the driver module used.
        Usage:       driver = conn.driver
        '''
        return self.__driver

    @property
    def auto_increment(self):
        '''
        Property:    auto_increment
        Description: Returns a boolean that indicates if the auto increment feature is enabled.
        Parameters:
            self:    The current object reference.
        Return:      A boolean (True or False).
        Usage:       auto_increment = conn.auto_increment
        '''
        return self.__auto_increment

    @auto_increment.setter
    def auto_increment(self, auto_increment):
        '''
        Property:           auto_increment
        Description:        Sets the value of the attribute that indicates if the auto increment feature is enabled.
        Parameters:
            self:           The current object reference.
            auto_increment: A value (True or False).
        Return:             none.
        Usage:              conn.auto_increment = True
        '''
        self.__auto_increment = auto_increment

    @property
    def auto_commit(self):
        '''
        Property:    auto_commit
        Description: Returns a boolean (True or False) that indicates if the auto_commit feature is enabled.
        Parameters:
            self:    The current object reference.
        Return:      A boolean value (True or False).
        Usage:       auto_commit = conn.auto_commit
        '''
        return self.__auto_commit
        
    @auto_commit.setter
    def auto_commit(self, auto_commit):
        '''
        Property:        auto_commit
        Description:     Sets the value of the attribute that indicates if the auto commit feature is enabled.
        Parameters:
            self:        The current object reference.
            auto_commit: A boolean (True or False).
        Return:          none.
        Usage:           conn.auto_commit = True
        '''
        self.__auto_commit = auto_commit

    @property
    def reference(self):
        '''
        Property:    reference
        Description: Returns a reference (memory address) to the connection.
        Parameters:
            self:    The current object reference
        Return:      A reference to the connection.
        Usage:       conn_ref = conn.reference
        '''
        return self.__reference

    @property
    def cursor(self):
        '''
        Property:    cursor
        Description: Returns a reference to the cursor of this connection.
        Parameters:
            self:    The current object reference.
        Return:      The cursor reference.
        Usage:       cur = conn.cursor
        '''
        return self.__cursor

    def commit(self):
        '''
        Method:      commit
        Description: Commit (does) the transaction (set of operations) in the database.
        Parameters:
            self:    The current object reference.
        Return:      none.
        Usage:       conn.commit()
        '''
        if self.reference:
            self.reference.commit()

    def rollback(self):
        '''
        Method:      rollback
        Description: Rollback (undoes) the transaction (set of operations) in the database.
        Parameters:
            self:    The current object reference.
        Return:      none.
        Usage:       conn.rollback()
        '''
        if self.reference:
            self.reference.rollback()

    def close(self):
        '''
        Method:      close
        Description: Closes the current connection.
        Parameters:
            self:    The current object reference.
        Return:      none.
        Usage:       conn.close()
        '''
        if self.cursor:
            self.cursor.close()
        if self.reference:
            self.reference.close()

class MySQLConnection(Connection):
    '''
    Class:       MySQLConnection
    Module:      pydao.db
    Description: Represents database connections to MySQL database server (DBMS - DATABASE MANAGEMENT SYSTEM).
    '''
    def __init__(
        self, host=None, port=None, user=None, password=None,
        database=None, auto_increment=False, auto_commit=False
    ):
        if not host:
            host = 'localhost'
        if not port:
            port = '3306'
        if not database:
            database = 'mysql'
        Connection.__init__(
            self, driver_name='MySQLdb', dbms='mysql',
            host=host, port=port, user=user, password=password,
            database=database, auto_increment=auto_increment,
            auto_commit=auto_commit
        )
        if self.host and self.port and self.user and self.password and self.database:
            self.__reference = self.driver.connect(
                host=self.host, port=int(self.port), user=self.user,
                passwd=self.password, db=self.database
            )
            # NOTE: For the auto commit feature works it is necessary that the engine of a table is 
            # InnoDB because this have support to transactions.
            # The MyISAM and ISAM engines doesn´t support transactions.
            # MyISAM is the default engine.
            # Talvez a linha abaixo seja desnecessaria uma vez que o construtor ja configura esse atributo
            self.auto_commit = auto_commit

    @Connection.auto_commit.setter
    def auto_commit(self, auto_commit):
        Connection.auto_commit = auto_commit
        self.__reference.autocommit(auto_commit)

    @property
    def reference(self):
        return self.__reference

    @property
    def cursor(self):
        return self.reference.cursor(self.driver.cursors.DictCursor)


class PostgreSQLConnection(Connection):
    '''
    Class:       PostgreSQLConnection
    Module:      pydao.db
    Description: Represents database connections to PostgreSQL database server (DBMS - DATABASE MANAGEMENT SYSTEM).
    
    '''
    def __init__(
        self, host=None, port=None, user=None, password=None,
        database=None, auto_increment=False, auto_commit=False
    ):
        if not host:
            host = 'localhost'
        if not port:
            port = '5432'
        if not database:
            database = 'postgres'
        Connection.__init__(
            self, driver_name='psycopg2', dbms='postgresql',
            host=host, port=port, user=user, password=password,
            database=database, auto_increment=auto_increment,
            auto_commit=auto_commit
        )
        if self.host and self.port and self.user and self.password and \
           self.database:
            self.__reference = self.driver.connect(
                host=self.host, port=self.port, user=self.user,
                password=self.password, dbname=self.database
            )
            self.auto_commit = auto_commit

    @Connection.auto_commit.setter
    def auto_commit(self, auto_commit):
        Connection.auto_commit = auto_commit
        self.__reference.autocommit = auto_commit
        # ISOLATION_LEVEL_AUTOCOMMIT 0
        # ISOLATION_LEVEL_READ_COMMITED 1 (Default)
        # if auto_commit equals True then sets_isolation_level = 0
        # else set_isolation level equals 1 sets self.__reference.set_isolation_level(not int(auto_commit))

    @property
    def reference(self):
        return self.__reference

    @property
    def cursor(self):
        # The line below makes a dynamic import it is the same that 
        # from psycopg2.extras import RealDictCursor
        module = __import__('%s.extras' % self.driver_name, globals(), locals(), ['RealDictCursor'], -1)
        return self.reference.cursor(cursor_factory=module.RealDictCursor)


class OracleConnection(Connection):
    '''
    Class:       OracleConnection
    Module:      pydao.db
    Description: Represents database connections to Oracle database server (DBMS - DATABASE MANAGEMENT SYSTEM).
    '''
    def __init__(
        self, host=None, port=None, user=None, password=None,
        database=None, auto_increment=False, auto_commit=False
    ):
        if not host:
            host = 'localhost'
        if not port:
            port = '1521'
        if not database:
            database = 'xe'
        Connection.__init__(
            self, driver_name='cx_Oracle', dbms='oracle',
            host=host, port=port, user=user, password=password,
            database=database, auto_increment=auto_increment,
            auto_commit=auto_commit
        )
        if self.host and self.port and self.user and self.password \
           and self.database:
            conn_str = '%s/%s@%s:%s/%s' % (
                self.user, self.password, self.host, self.port, self.database
            )
            self.__reference = self.driver.connect(conn_str)
            self.auto_commit = auto_commit

    @Connection.auto_commit.setter
    def auto_commit(self, auto_commit):
        Connection.auto_commit = auto_commit
        self.__reference.autocommit = auto_commit

    @property
    def reference(self):
        return self.__reference

    @property
    def cursor(self):
        return self.reference.cursor()


class SQLiteConnection(Connection):
    '''
    Class:       SQLiteConnection
    Module:      pydao.db
    Description: Represents database connections to SQLite standalone database (DBMS - DATABASE MANAGEMENT SYSTEM).
    '''
    def __init__(self, database, auto_increment=False, auto_commit=False):
        Connection.__init__(
            self, driver_name='sqlite3', sgbd='sqlite',
            host=None, port=None, user=None, password=None,
            database=database, auto_increment=auto_increment,
            auto_commit=auto_commit
        )
        if self.database:
            self.__reference = self.driver.connect(database)
            self.auto_commit = auto_commit

    @Connection.auto_commit.setter
    def auto_commit(self, auto_commit):
        Connection.auto_commit = auto_commit
        if auto_commit:
            self.__reference.isolation_level = None
        else:
            self.__reference.isolation_level = 'DEFERRED'

    @property
    def reference(self):
        return self.__reference

    @property
    def cursor(self):
        return self.reference.cursor()


class DriverManager(object):
    '''
    Class:        DriverManager
    Module:       pyorm.db
    Description:  This class is responsible for creating connections for the 
                  specified database server (DBMS - DATABASE MANAGEMENT SYSTEM).
    '''

    def __init__(self):
        pass

    def connection(self, dbms=None, host=None, port=None, user=None,
                   password=None, database=None, auto_increment=False,
                   auto_commit=False
    ):
        if dbms:
            dbms = dbms.lower()
            conn = None
            if dbms == 'mysql':
                conn = MySQLConnection(
                    host=host, port=port, user=user,
                    password=password, database=database,
                    auto_increment=auto_increment, auto_commit=auto_commit
                )
            elif dbms == 'postgresql':
                conn = PostgreSQLConnection(
                    host=host, port=port, user=user, password=password,
                    database=database, auto_increment=auto_increment,
                    auto_commit=auto_commit
                )
            elif dbms == 'oracle':
                conn = OracleConnection(
                    host=host, port=port, user=user, password=password,
                    database=database, auto_increment=auto_increment,
                    auto_commit=auto_commit
                )
            elif dbms == 'sqlite':
                conn = SQLiteConnection(
                    database=database, auto_increment=auto_increment,
                    auto_commit=auto_commit
                )
            else:
                pass
            return conn

    # Creating a static method with classmethod function.
    connection = classmethod(connection)

if __name__ == '__main__':
    conn = DriverManager.connection(dbms='mysql', host='localhost', user='pydao', password='pydao', database='pydao')
    print conn
