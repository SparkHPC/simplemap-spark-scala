# 
# Note: Proof of concept. Not in any way ready for primetime!
#

import sqlite3
import sys


def dict_factory(cursor, row):
  d = {}
  for idx, col in enumerate(cursor.description):
    d[col[0]] = row[idx]
  return d

Q="select distinct(block_size) from results"

conn = sqlite3.connect("simplemap-spark-scala.db")
conn.execute(Q)

conn.row_factory = dict_factory

cursor = conn.execute(Q)

block_sizes = [ b['block_size'] for b in cursor]
block_sizes.sort()
print(block_sizes)

Q2 = "select * from results where block_size = %(b)d and nodes * cores * nparts == blocks and nparts=1 order by nodes"

by_block = {}
def linestyle_gen():
   while True:
     yield "bo--"
     yield "go--"
     yield "ro--"
     yield "yo--"

for b in block_sizes:
	Q3=Q2 % vars()
	cursor = conn.execute(Q3)
	by_block[b] = [{ 'nodes' : result['nodes'],
                         'map_time' : result['map_time'],
                         'shift_time' : result['shift_time']}
                       for result in cursor]
        print("Number of items in %d = %d" % (b, len(by_block[b])))

import matplotlib.pyplot as plt
plt.xlabel('Nodes')
plt.ylabel('Time (x10^11)')
linestyle = linestyle_gen()
for b in block_sizes:
   nodes = [ d['nodes'] for d in by_block[b]]
   map_time = [ d['map_time'] for d in by_block[b]]
   plt.plot(nodes, map_time, linestyle.next(), linewidth=1)
   #plt.axis([0, 6, 0, 20])

plt.show()
