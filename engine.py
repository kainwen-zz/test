#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import pygresql.pg as pg


class Context(object):

    def __init__(self, hostname, port):
        self.conn = pg.connect(host=hostname,
                               port=port,
                               dbname='gpadmin')

    def execute(self, sql):
        r = self.conn.query(sql)
        if hasattr(r, "getresult"):
            return r.getresult()
        else:
            return r
