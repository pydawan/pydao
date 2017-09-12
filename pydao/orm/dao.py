#!/usr/bin/env python
#-*- coding: utf-8 -*-

'''
Created on 28/03/2014

@author: thiago-amm
'''

__author__ = 'Thiago Alexandre Martins Monteiro'
__date__ = '24/11/2012'

class GenericDAO(object):
    '''
    Class:       GenericDAO
    Module:      pydao.dao
    Description: Provide access to data through the DAO (DATA ACCESS OBJECT) design pattern
                 isolating the persistence layer from the business layer of the application.
    '''
    def __init__(self, connection=None, model=None, dic={}, **entries):
        '''
        Method:      __init__
        Description: Constructor that initializes objects of this class. 
        Parameters:
            self:    The current object reference.
        Return:      none.
        Usage:       gd = GenericDAO(connection=conn, model=Product)
        '''
        # None
        self.__model = model
        if model:
            self.__model = model()
#        if dic and dic.keys():
#            # Create an model class (class Model(object) ) in the project
#            self.__model.__dict__.update(dic)
#        if entries and entries.keys():
#            self.__model.__dict__.update(entries)
        self.update_model_name()
        self.__connection = connection

    @property
    def connection(self):
        '''
        Property:    connection
        Description: Returns a reference to the database connection used by this object.
        Parameters:
            self:    The current object reference.
        Return:      A reference to the database connection used by this object.
        Usage:       conn = gd.connection
                     NOTE: See the description of the constructor (method __init__) of this class above to
                     understand what the gd variable is.
        '''
        return self.__connection

    @connection.setter
    def connection(self, connection):
        '''
        Property:    connection
        Description: Sets a reference to a database connection that will be used by this object.
        Parameters:
            self:    The current object reference.
        Return:      none.
        Usage:       gd.connection  = DriverManager.connection(dbms='mysql', user='root', password='root', database='mysql')
                     NOTE: See the description of the connection method in the DriverManager class of the pydao.db module
                     to understand what it does.
        '''
        self.__connection = connection

    @property
    def model(self):
        '''
        Property:    model
        Description: Returns a reference to the model object used by this object.
        Parameters:
            self:    The current object reference.
        Return:      A reference to the model object used by this object.
        Usage:       model = gd.model
                     NOTE: See the description of the constructor (method __init__) of this class above to
                     understand what the gd variable is.
        '''
        return self.__model

    @model.setter
    def model(self, model):
        '''
        Property:    model
        Description: Sets the model object of this object.
        Parameters:
            self:    The current object reference.
        Return:      none.
        Usage:       gd.model = Product
                     NOTE: See the description of the constructor (method __init__) of this class above to
                     understand what the gd variable is.
        '''
        self.__model = model()
        self.update_model_name()

    @property
    def model_name(self):
        '''
        Property:    model_name
        Description: Returns the name of the model object used by this object.
        Parameters:
            self:    The reference to this object.
        Return:      The name of the model object used by this object.
        Usage:       model_name = gd.model_name
                     NOTE: See the description of the constructor (method __init__) of this class above to
                     understand what the gd variable is.
        '''
        return self.__model_name

    @model_name.setter
    def model_name(self, model_name):
        '''
        Property:    model_name
        Description: Sets the name of the model used by this object.
        Parameters:
            self:    The reference to this object.
        Return:      none.
        Usage:       gd.model_name = 'Clients'
                     NOTE: See the description of the constructor (method __init__) of this class above to
                     understand what the gd variable is.
        '''
        self.__model_name = model_name
        self.update_model()

    def update_model_name(self):
        '''
        Method:      update_model_name
        Description: Updates the name of the model object used by this object.
        Parameters:
            self:    The reference to this object.
        Return:      none.
        Usage:       gd.update_model_name()
                     NOTE: See the description of the constructor (method __init__) of this class above to
                     understand what the gd variable is.
        '''
        if self.model:
            uppers = filter(str.isupper, self.__model.__class__.__name__)
            if len(uppers) == 1:
                self.__model_name = '%ss' % self.__model.__class__.__name__.lower()
            elif len(uppers) > 1:
                name = list(self.__model.__class__.__name__)
                name_aux = name[:]
                offset = 0
                for i, c in enumerate(name_aux):
                    if i > 0 and c in uppers:
                        name.insert(i + offset, 's_')
                        offset += 1
                name.append('s')
                name = ''.join(name)
                self.__model_name = name.lower()
            else:
                pass
        else:
            self.__model_name = None

    def update_model(self):
        '''
        Method:      update_model
        Description: Updates the model object used by this object.
        Parameters:
            self:    The reference to this object.
        Return:      none.
        Usage:       gd.update_model()
                     NOTE: See the description of the constructor (method __init__) of this class above to
                     understand what the gd variable is.
        '''
        if self.__model_name:
            try:
                # Import the model.
                module = __import__('%s.%s' % ('models', self.__model_name), globals(), locals(), [], -1)
                # Create a instance of the model class.
                self.__model = module()
            except Exception, e:
                print e.message

    def select(self, fields=[], where=''):
        '''
        Method:         select
        Description:    Returns a set of rows from a table with represents a model class.
        Parameters:
            self:       The current object reference.
            fields:     A list (default a empty list - []).
            conditions: A string (default a empty string - '').
        Return:         A list - [].
        Usage:          gd.select(fields=['name', 'price'], where='id < 3 and price = 20')
                        NOTE: See the description of the constructor (method __init__) of this class above to
                        understand what the gd variable is.
        '''
        if self.connection.reference and self.model:
            cursor  = self.connection.cursor
            sql = 'SELECT %s FROM '
            if fields and (type(fields) == type([])):
                attrs = ''
                for f in fields:
                    attrs += '%s, ' % f
                # Remove the blank space and the comma of the end.
                attrs = attrs[:-2]
                sql = sql % attrs
            else:
                sql = sql % '*'
            # Add the table name in the SQL statement
            # obeying the rules for naming tables.
            # To wit:
            # The entity name in plural and lowercase.
            # Ex: product (Product model class) entity => products table.
            sql = '%s %s' % (sql, self.model_name)

            if where and (type(where) == type('') ):
                sql = '%s WHERE %s' % (sql, where)
            #print sql
            try:
                cursor.execute(sql)
            except Exception, e:
                print e.message
            return cursor.fetchall()
        else:
            return None

    def insert(self, obj=None, settings={}):
        '''
        Method:       insert
        Description:  Insert a register in the table that represents the model class in the database.
        Parameters:
            self:     The current object reference.
            obj:      The object that will be insert (default None).
            settings: A dictionary with the data that will be inserted (default a empty dictionary - {}).
            NOTE:     You can use a object or a dictionary but don´t both.
        Return:       none.
        Usage:        gd.insert(obj) or 
                      gd.insert({'name': 'Personal Computer', 'price': '500'})
                      NOTE: See the description of the constructor (method __init__) of this class above to
                      understand what the gd variable is.
        '''
        if self.connection.reference and self.model:
            cursor = self.connection.cursor
            sql = 'INSERT INTO %s' % self.__model_name
            if obj and isinstance(obj, self.model.__class__):
                fields = ''
                for f in obj.__dict__.keys():
                    f = f.replace('_%s__' % obj.__class__.__name__, '')
                    if not (self.connection.auto_increment and f == 'id'):
                        fields += '%s, ' % f
                # Remove the blank space and comma of the end.
                fields = '(%s)' % fields[:-2]
                # print fields
                values = ''
                for attr, val in obj.__dict__.items():
                    attr = attr.replace('_%s__' % obj.__class__.__name__, '')
                    if self.connection.auto_increment and attr == 'id':
                        pass
                    else:
                        values += "'%s', " % val
                values = '(%s)' % values[:-2]
                if fields and values:
                    sql = "%s %s VALUES %s" % (sql, fields, values)
            # Receive a dictionary with the data to be registered.
            else:
                dic = settings
                if self.check_dict(dic):
                    settings = ''
                    fields = ''
                    for f in dic.keys():
                        fields += '%s, ' % f
                    fields = fields[:-2]
                    values = ''
                    for v in dic.values():
                        values += "'%s', " % v
                    values = values[:-2]
                    sql = '%s (%s) VALUES (%s)' % (sql, fields, values)
            #print sql
            try:
                cursor.execute(sql)
                if obj:
                    obj.id = self.last_id()
                if settings:
                    settings['id'] = self.last_id()
            except Exception, e:
                print e.message

    def update(self, obj=None, settings={}, where={}):
        '''
        Method:       update
        Description:  Updates a register in a table on the database that represents the model object used by this object.
        Parameters:
            self:     The reference to this object.
            obj:      The object representing the register that will be updated (default None).
            settings: A dictionary with the update data (default a empty dictionary - {}).
            where:    A dictionary with the conditions for the update happen (default a empty dictionary - {}).
        Return:       none.
        Usage:        gd.update(obj=p, where={'name': 'Personal Computer'}) or
                      gd.update({'name': 'Ipod', 'price': '150'}, where={'name': 'Personal Computer'})
                      NOTE: See the description of the constructor (method __init__) of this class above to
                      understand what the gd variable is.
        '''
        if self.connection.reference and self.model:
            cursor = self.connection.cursor
            sql = 'UPDATE %s SET' % self.__model_name
            # Receive a object.
            if obj and isinstance(obj, self.model.__class__):
                settings = ''
                for attr, val in obj.__dict__.items():
                    attr = attr.replace('_%s__' % obj.__class__.__name__, '')
                    if attr <> 'id' and val:
                        settings += "%s = '%s', " % (attr, val)
                # Remove the blank space and comma of the settings end.
                settings = settings[:-2]
                sql = "%s %s WHERE id = '%s'" % (sql, settings, obj.id)
            # Receive two dictionaries (
            else: # Receber dois dicionarios (one with the settings and the other with conditions).
                dic = settings
                if self.check_dict(dic):
                    settings = ''
                    for k, v in dic.items():
                        if v in [[], {}, None]:
                            settings += "%s = 'NULL'" % k
                        elif v == False:
                            settings += "%s = '0', " % k
                        else:
                            settings += "%s = '%s', " % (k, v)
                    settings = settings[:-2]
                    sql = '%s %s' % (sql, settings)
                dic = where
                if self.check_dict(dic):
                    where = ''
                    for k, v in dic.items():
                        where += "%s = '%s' AND " % (k, v)
                    where = where[:-4]
                    sql = '%s WHERE %s' % (sql, where)
            #print sql
            try:
                cursor.execute(sql)
            except Exception, e:
                print e.message

    def delete(self, obj=None, where={}):
        '''
        Method:      delete
        Description: Deletes the register in a database table that represents the object model of this object.
        Parameters:
            self:    The reference to this object.
        Return:      none.
        Usage:       gd.delete(obj=p, where={'id': 1})
                     NOTE: See the description of the constructor (method __init__) of this class above to
                     understand what the gd variable is.
        '''
        if self.connection.reference and self.model:
            cursor = self.connection.cursor
            sql = 'DELETE FROM %s WHERE' % self.__model_name
            if obj and isinstance(obj, self.model.__class__):
                sql = "%s id = '%s'" % (sql, obj.id)
            else:
                # Check if the dictionary passed is valid to the associated model.
                if self.check_dict(where):
                    conditions = ''
                    for f, v in where.items():
                        conditions += "%s = '%s' AND " % (f, v)
                    conditions = conditions[:-4]
                    sql = '%s %s' % (sql, conditions)
            #print sql
            try:
                cursor.execute(sql)
            except Exception, e:
                print e.message

    def last_id(self):
        '''
        Method:      last_id
        Description: Returns the value of the id column from the last register inserted in the table 
                     that represents the object model used by this object.
        Parameters:
            self:    The reference to this object.
        Return:      A integer value - int
        Usage:       gd.last_id()
        '''
        id = None
        if self.connection.reference and self.model:
            sql = ''
            if  self.connection.dbms.lower() == 'mysql' or \
                self.connection.dbms.lower() == 'postgresql' or \
                self.connection.dbms.lower() == 'sqlite':
                    # Id of the last inserted record.
                    sql = 'SELECT id FROM %s ORDER BY id DESC LIMIT 1'
                    sql = sql % self.__model_name
            elif self.connection.dbms.lower() == 'oracle':
                sql = 'SELECT MAX(id) FROM %s'
                sql = sql % self.__model_name
            else:
                pass
            #print sql
            try:
                cursor = self.connection.cursor
                cursor.execute(sql)
                dbms = self.connection.dbms.lower()
                if dbms in ['mysql', 'postgresql']:
                    id = cursor.fetchone()['id']
                elif dbms in ['oracle', 'sqlite']:
                    id = cursor.fetchone()[0]
            except Exception, e:
                print e.message
        return id

    def check_dict(self, dic):
        '''
        Method:      check_dict
        Description: Check if the keys of the dictionary matches to the attributes of the class
                     from the model layer associated with this object.
        Parameters:
            self:    The reference to this object.
            dic:     A dictionary - {}.
        Return:      Returns a boolean value (True or False).
        Usage:       gd.check_dict({'id': 0, 'name': ''})
                     NOTE: See the description of the constructor (method __init__) of this class above to
                     understand what the gd variable is.
        '''
        if self.model:
            # Check if a dictionary was passed with something.
            if dic and (type(dic) == type({})):
                for k in dic.keys():
                    # Show the key that not matchs with the model class attribute.
                    #print k
                    if not self.model.__dict__.has_key('_%s__%s' % (self.model.__class__.__name__, k)):
                        return False
            else:
                return False
        else:
            return False
        return True

    def fill(self, obj=None, dic={}):
        '''
        Method:       fill
        Description:  Fill a object or a dictionary argument with the data from a database table that represents
                      the object model used by this class.
        Parameters:
            self:     The referente to this object.
        Return:       none.
        Requirements: Assign a value to the attribute id of the dictionary or object passed as argument to this method.
        Usage:        gd.fill(obj=p) or
                      gd.fill(dic={'id': 1})
                      NOTE: See the description of the constructor (method __init__) of this class above to
                      understand what the gd variable is.
        '''
        if not self.connection.reference:
            return
        if obj and isinstance(obj, self.model.__class__):
            # Returns a dictionary matching with a tuple on the table.
            dic = self.select(where="id = '%s'" % obj.id)[0]
            # Copy the values of dictionary above to the object.
            for k, v in dic.items():
                k = '_%s__%s' % (self.model.__class__.__name__, k)
                obj.__dict__[k] = v
        else:
            if self.check_dict(dic):
                reg = self.select(where="id = '%s'" % dic['id'])[0]
                # Copy the values of a dictionary to other.
                for k, v in reg.items():
                    dic[k] = v


if __name__ == '__main__':
    from pydao.orm.models import Produto
    p = Produto()
    p.id = 1
    p.nome = 'Produto 1'
    p.preco = 300.0
    from db import DriverManager
    dm = DriverManager()
    conn = dm.connection(dbms='mysql', user='pydao', password='pydao', database='pydao')
    # Dar a opção de passar um dicionario no lugar do objeto.
    gd = GenericDAO(conn, Produto, auto_increment=True)
    for r in gd.select():
        print r
