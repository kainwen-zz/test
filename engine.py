#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import pygresql.pg as pg


class Context(object):

    def __init__(self, configs):
        self.configs = configs
        self.conns = [pg.connect(host=hostname,
                                 port=port,
                                 dbname='gpadmin')
                      for hostname, port in configs]
        self.conn = self.conns[0]

    def execute(self, sql):
        r = self.conn.query(sql)
        if hasattr(r, "getresult"):
            return r.getresult()
        else:
            return r
