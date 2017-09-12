#!/usr/bin/env python
#-*- coding: utf-8 -*-

'''
Created on 28/03/2014

@author: thiago-amm
'''

class Produto(object):
    def __init__(self):
        self.__id = None
        self.__nome = None
        self.__preco = None

    @property
    def id(self):
        return self.__id

    @id.setter
    def id(self, id):
        self.__id = id

    @property
    def nome(self):
        return self.__nome

    @nome.setter
    def nome(self, nome):
        self.__nome = nome

    @property
    def preco(self):
        return self.__preco

    @preco.setter
    def preco(self, preco):
        self.__preco = preco

    # Like toString() method in Java.
    def __str__(self):
        return '%s\nNome: %s\nPreço: %s' % (str(self.__id), str(self.__nome), str(self.__preco) )