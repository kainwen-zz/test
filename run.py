from sqlgen import SQLGen
from engine import Context


context = Context('zlv', 15432)
context.execute('set allow_system_table_mods = true')

s = SQLGen(context, 5)
s.build_tables()

jt = s.make_join()
print s.make_groupby()
