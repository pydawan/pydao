#!/usr/bin/env python
#-*- encoding: utf-8 -*-

'''
Created on 28/03/2014

@author: thiago-amm
'''

__author__ = 'Thiago Alexandre Martins Monteiro'
__date__ = '23/11/2012'

from db import DriverManager as dm
import dao

if __name__ == '__main__':
    from pydao.orm.models import Produto
    # Creating a model class instance.
    produto = Produto()
    produto.id = 1
    produto.nome = 'CD George Michael'
    produto.preco = 21.00    
    print '%s' % produto    
    conn = dm.connection(dbms='mysql', user='pydao', password='pydao', database='pydao')
    print conn
    # Creating a instance of the GenericDAO class.
    dao = dao.GenericDAO(model=Produto, connection=conn)
    dao.insert(produto)
#    for r in dao.select():
#        print r
    conn.close()
