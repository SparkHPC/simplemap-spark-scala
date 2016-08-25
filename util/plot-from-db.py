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

# TODO: Need to get a list of known linestyles w/o hard coding them.

# This just generates different line color/shapes for each line being ploted.

def linestyle_gen():
   for (color, shape) in zip("bgrcmyk"*100, "o^*8s"*100):
      yield color + shape + "--"

for b in block_sizes:
	Q3=Q2 % vars()
	cursor = conn.execute(Q3)
	by_block[b] = [
    { 'nodes' : result['nodes'],
      'map_time' : result['map_time'] / 1.0e11,
      'shift_time' : result['shift_time'] / 1.0e11,
      'avg_time' : result['shift_time'] / 1.0e11
    } for result in cursor]
  #print("Number of items in %d = %d" % (b, len(by_block[b])))

def create_report(name):
  import matplotlib.pyplot as plt
  plt.xlabel('Nodes')
  plt.ylabel('Time (x10^11)')
  plt.title("Performance (%s) by Nodes, Block Size (x Nodes x Cores/Node)" % name)
  linestyle = linestyle_gen()
  legendlines = []
  for b in block_sizes:
    nodes = [ d['nodes'] for d in by_block[b]]
    t = [ d[name] for d in by_block[b]]
    line, = plt.plot(nodes, t, linestyle.next(), label="%(b)d MB" % vars(), linewidth=1)

    legendlines.append(line)
    plt.legend(handles=legendlines, loc=1)
    #plt.axis([0, 6, 0, 20])

  plt.savefig('%s.png' % name)
  plt.close()

create_report('map_time')
create_report('avg_time')
create_report('shift_time')
