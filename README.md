# Generate SQL for table_expand test

## GOAL

In the feature `table_expand` branch, we add a column in the catalog gp_policy.
Now tables in a greenplum db cluster can have different numsegments. So we have
to modify planner & executor to support new kinds of motions.

This script aims to randomly generating SQL to help test.

## Usage

* `sqlgen.py:SQLgen.build_tables`: create table, set numsegments and insert data
* `sqlgen.py:SQLgen.make_join`: return a JoinTree object that can dump a join-sql text
* `sqlgen.py:SQLgen.make_group_by`: return a sql with the pattern "select c1, c2, agg(c3), agg(c4) from (t1 join t2 ....) group by c1, c2"

## Example

```bash
~ # make sure you have a running gpdb cluster with correct branch
~ createdb gpadmin # make sure you have this db
```

```python
In [1]: cat run.py
from sqlgen import SQLGen
from engine import Context


context = Context('zlv', 15432)
context.execute('set allow_system_table_mods = true')

s = SQLGen(context, 5)
s.build_tables()

jt = s.make_join()
jt.dump()
print s.make_groupby()

In [2]: from sqlgen import SQLGen

In [3]: from engine import Context

In [4]: context = Context('zlv', 15432)

In [5]: context.execute('set allow_system_table_mods = true')

In [6]: s = SQLGen(context, 5)

In [7]: s.build_tables()

In [8]: jt = s.make_join()

In [9]: jt.dump()
Out[9]: '((replicate_3_0 right join random_2_1 on replicate_3_0.a < random_2_1.c) left join replicate_2_2 on replicate_3_0.d = replicate_2_2.b) left join (random_3_3 right join hash_1_2_4 on random_3_3.b >= hash_1_2_4.b) on replicate_3_0.c < random_3_3.a'

In [10]: s.make_group_by()
Out[10]: 'select random_2_1.c, replicate_3_0.a, avg(replicate_2_2.a), avg(hash_1_2_4.d) from (replicate_3_0 full join random_2_1 on replicate_3_0.b <> random_2_1.b) right join ((replicate_2_2 right join random_3_3 on replicate_2_2.a < random_3_3.b) left join hash_1_2_4 on replicate_2_2.a <> hash_1_2_4.c) on replicate_3_0.d < hash_1_2_4.d group by random_2_1.c, replicate_3_0.a'
```