"""
You had better run this after you have read data in POSTGIS

We have two layer

buildings
gas_pipes
"""

import psycopg2

dbname = "00_NCL_WATER_TEMP_CATCHMENT"
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
cur = conn.cursor()

cur.execute("alter table buildings drop column if exists bid")
cur.execute("alter table buildings add column bid integer")
print("add bid column done")
cur.execute("update buildings set bid = gid - 1")
print("update bid done")

cur.execute("alter table buildings drop column if exists cp")
cur.execute("alter table buildings add column cp geometry(Point, 27700)")
print("add cp column done")
cur.execute("update buildings set cp = st_centroid(geom)")
print("update cp done")

"""
# make sure you change the toid_numbe to toid
cur.execute("alter table buildings add column toid varchar(80)")
print("add column toid done")
cur.execute("update buildings set toid = toid_numbe")
print("update toid done")
"""

cur.execute("alter table water_pipes add column se_pid serial")
cur.execute("alter table water_pipes drop column if exists pid")
cur.execute("alter table water_pipes add column pid integer")
print("add column pid done")
cur.execute("update water_pipes set pid = se_pid - 1")
print("update pid done")

conn.commit()

cur.close()
conn.close()
