from sqlgen import SQLGen
from engine import Context


context = Context([('zlv', 15432)])
context.execute('set allow_system_table_mods = true')

s = SQLGen(context, 4)
s.build_tables()

while True:
    jt = s.make_join_from_tablist(s.tables)
    print jt.dump()
    flag = raw_input("continue?...")
    if flag == "yes":
        break
