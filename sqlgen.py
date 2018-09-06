#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import random


class SQLGen(object):
    
    def __init__(self, context, Ntabs):
        self.context = context;
        self.Ntabs = Ntabs
        self.tables = self.gen_tables()

    def gen_tables(self):
        tabs = [Table.gen_table(self.context, i)
                for i in range(self.Ntabs)]
        return tabs

    def build_tables(self):
        for tab in self.tables:
            tab.create()
            tab.set_numsegs()
            tab.insert()

    def pick_col_from_tabs(self, tabs):
        tab = random.choice(tabs)
        return tab.pick_col()

    def make_join_tree(self):
        jt = self.make_join_from_tablist(self.tables)
        return jt

    def make_join_from_tablist(self, tablist):
        if not tablist:
            return None
        if len(tablist) == 1:
            return JoinTree(None, None, None, None, tablist[0])
        
        part_point = random.randint(1, len(tablist)-1)
        left_tabs, right_tabs = tablist[:part_point], tablist[part_point:]
        left = self.make_join_from_tablist(left_tabs)
        right = self.make_join_from_tablist(right_tabs)
        join_type = JoinType.gen_join_type()
        join_cond = JoinCond.gen_join_cond([self.pick_col_from_tabs(left_tabs),
                                            self.pick_col_from_tabs(right_tabs)],
                                           join_type.join_type == JoinType.JOIN_TYPE_RIGHT)
        return JoinTree(join_type, left, right, join_cond, None)



############### Abstract Syntax Tree #################
# Join :: {Single_tab, <tab>}
#       | {Join_type, left::Join, right::Join}.
class JoinType(object):

    JOIN_TYPE_INNER = 0
    JOIN_TYPE_FULL = 1
    JOIN_TYPE_LEFT = 2
    JOIN_TYPE_RIGHT = 3

    JOIN_TYPE_NAME_MAP = dict([(JOIN_TYPE_INNER, "inner join"),
                               (JOIN_TYPE_FULL, "full join"),
                               (JOIN_TYPE_LEFT, "left join"),
                               (JOIN_TYPE_RIGHT, "right join")])
    
    def __init__(self, join_type):
        self.join_type = join_type

    @classmethod
    def gen_join_type(cls):
        return JoinType(random.choice([cls.JOIN_TYPE_INNER,
                                       cls.JOIN_TYPE_FULL,
                                       cls.JOIN_TYPE_LEFT,
                                       cls.JOIN_TYPE_RIGHT]))

    def dump(self):
        return self.JOIN_TYPE_NAME_MAP[self.join_type]
    

class JoinCond(object):

    JOIN_COND_EQ = 0
    JOIN_COND_GT = 1
    JOIN_COND_LT = 2
    JOIN_COND_GE = 3
    JOIN_COND_LE = 4
    JOIN_COND_NE = 5

    JOIN_COND_NAME_MAP = dict([(JOIN_COND_EQ, "="),
                               (JOIN_COND_GT, ">"),
                               (JOIN_COND_LT, "<"),
                               (JOIN_COND_GE, ">="),
                               (JOIN_COND_LE, "<="),
                               (JOIN_COND_NE, "<>")])

    def __init__(self, operator, left, right):
        self.operator = operator
        self.left = left
        self.right = right

    @classmethod
    def gen_join_cond(cls, cols, only_eq=False):
        assert len(cols) == 2
        left, right = cols
        operator = random.choice([cls.JOIN_COND_EQ,
                                  cls.JOIN_COND_GT,
                                  cls.JOIN_COND_LT,
                                  cls.JOIN_COND_GE,
                                  cls.JOIN_COND_LE,
                                  cls.JOIN_COND_NE])
        return JoinCond(operator, left, right)

    def dump(self):
        return "{left} {op} {right}".format(op=self.JOIN_COND_NAME_MAP[self.operator],
                                            left=self.left,
                                            right=self.right)

    
class JoinTree(object):

    def __init__(self, join_type, left, right, join_cond, single_tab):
        self.join_type, self.left, self.right, self.join_cond, self.single_tab = \
          join_type, left, right, join_cond, single_tab
        assert self.is_single_tab() != self.is_real_join()

    def is_single_tab(self):
        return self.single_tab is not None

    def is_real_join(self):
        return (self.join_type is not None and
                self.left is not None and
                self.right is not None and
                self.join_cond is not None)

    def dump(self):
        if self.is_single_tab():
            return self.single_tab.name
        else:
            left_dump = self.left.dump() if self.left.is_single_tab() else ("(%s)" % self.left.dump())
            right_dump = self.right.dump() if self.right.is_single_tab() else ("(%s)" % self.right.dump())
            return "{left} {join_type} {right} on {join_cond}".format(left=left_dump,
                                                                      right=right_dump,
                                                                      join_type=self.join_type.dump(),
                                                                      join_cond=self.join_cond.dump())
            


class Table(object):
    """
    * hash dist
    * random dist
    * replicated
    * func scan
    """
    TABLE_TYPE_HASH = 0
    TABLE_TYPE_RANDOM = 1
    TABLE_TYPE_REPLICATE = 2
    TABLE_TYPE_FUNC = 3

    TABLE_TYPE_PREFIX_MAP = dict([(TABLE_TYPE_HASH, 'hash'),
                                (TABLE_TYPE_RANDOM, 'random'),
                                (TABLE_TYPE_REPLICATE, 'replicate'),
                                (TABLE_TYPE_FUNC, 'func')])

    def __init__(self, context, table_type, numsegs, id, hashkeys=None):
        self.table_type = table_type
        self.numsegs = numsegs
        self.id = id
        self.hashkeys = hashkeys
        if self.table_type == self.TABLE_TYPE_HASH:
            assert self.hashkeys is not None
        self.name = self.gen_name()
        self.schema = self.gen_schema()
        self.context = context

    @classmethod
    def gen_table(cls, context, id):
        table_type = random.choice([cls.TABLE_TYPE_HASH, cls.TABLE_TYPE_RANDOM,
                                    cls.TABLE_TYPE_REPLICATE, cls.TABLE_TYPE_FUNC])
        numsegs = random.choice([1, 2, 3])
        if table_type == cls.TABLE_TYPE_HASH:
            hashkeys = random.choice([1, 2, 3])
        else:
            hashkeys = None
        return Table(context, table_type, numsegs, id, hashkeys)

    def gen_name(self):
        prefix = self.get_table_type_prefix(self.table_type)
        name_elements = [prefix, str(self.numsegs)]
        if self.hashkeys is not None:
            name_elements.append(str(self.hashkeys))
        name_elements.append(str(self.id))
        name =  "_".join(name_elements)
        if self.table_type == self.TABLE_TYPE_FUNC:
            self.func_name = name
            return "generate_series(1, 20)%s" % name
        else:
            return name

    def gen_schema(self):
        self.cols = self.gen_cols()
        return zip(self.cols, ['int'] * len(self.cols))

    def gen_cols(self):
        if self.table_type == self.TABLE_TYPE_FUNC:
            return [self.func_name]
        else:
            return ['a', 'b', 'c', 'd']

    def pick_col(self):
        col = random.choice(self.cols)
        if self.table_type == self.TABLE_TYPE_FUNC:
            return col
        else:
            return "%s.%s" % (self.name, col)

    def get_table_type_prefix(self, table_type):
        return self.TABLE_TYPE_PREFIX_MAP[table_type]

    def create(self):
        if self.table_type == self.TABLE_TYPE_FUNC:
            return

        first_part_sql = "create table {tabname} ({schema})".format(tabname=self.name,
                                                                    schema=", ".join([("%s %s" % (col, tp))
                                                                                      for col, tp in self.schema]))
        if self.hashkeys:
            second_part_sql = "distributed by (%s)" % ",".join(self.cols[:self.hashkeys])
        elif self.table_type == self.TABLE_TYPE_RANDOM:
            second_part_sql = "distributed randomly"
        elif self.table_type == self.TABLE_TYPE_REPLICATE:
            second_part_sql = "distributed replicated"

        sql = first_part_sql + " " + second_part_sql

        return self.context.execute(sql)

    def insert(self):
        if self.table_type == self.TABLE_TYPE_FUNC:
            return
        
        sql = "insert into {tabname} select i,i,i,i from generate_series(1, 10)i".format(tabname=self.name)
        return self.context.execute(sql)

    def set_numsegs(self):
        if self.table_type == self.TABLE_TYPE_FUNC:
            return
        
        sql = "update gp_distribution_policy set numsegments = {numsegs} \
               where localoid = '{tabname}'::regclass".format(tabname=self.name,
                                                              numsegs=self.numsegs)
        return self.context.execute(sql)
