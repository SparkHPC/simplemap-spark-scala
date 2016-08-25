#
# Plot strong, weak, and data scaling data from performance results, which
# are collected in an ephemeral sqlite3 database.
# 

import sys
import sqlite3

def dict_factory(cursor, row):
  record = {}
  for idx, col in enumerate(cursor.description):
    record[col[0]] = row[idx]
  return record

def linestyle_iterator():
  for (color, shape) in zip("bgrcmyk"*100, "o^*8s"*100):
    yield color + shape + "--"

class PerfReport(object):
  def __init__(self):
    self.conn = sqlite3.connect("simplemap-spark-scala.db")
    self.conn.row_factory = dict_factory

  def block_sizes_iterator(self):
    query = "select distinct(block_size) from results"
    records = self.conn.execute(query)
    for r in records:
      yield r['block_size']

  def get_block_sizes(self):
    block_sizes = list(self.block_sizes_iterator())
    block_sizes.sort()
    return block_sizes

  def get_by_blocksize(self):
    query = """select * from results
               where block_size = %(block_size)d
                 and nodes * cores * nparts == blocks and nparts=1
               order by nodes"""

    by_blocksize = {}
    for block_size in self.get_block_sizes():
	cursor = self.conn.execute(query % vars())
	by_blocksize[block_size] = [
         { 'nodes' : result['nodes'],
           'map_time' : result['map_time'] / 1.0e9,
           'shift_time' : result['shift_time'] / 1.0e9,
           'avg_time' : result['shift_time'] / 1.0e9
         } for result in cursor ]
    return by_blocksize

  def create_report(self, name):
    import matplotlib.pyplot as plt
    plt.xlabel('Nodes')
    plt.ylabel('Time (seconds)')
    plt.title("Performance (%s) by Nodes, Block Size (x Nodes x Cores/Node)" % name)
    linestyle = linestyle_iterator()
    legendlines = []
    by_block = self.get_by_blocksize()
    for b in self.get_block_sizes():
      nodes = [ d['nodes'] for d in by_block[b]]
      t = [ d[name] for d in by_block[b]]
      line, = plt.plot(nodes, t, linestyle.next(), label="%(b)d MB" % vars(), linewidth=1)

      legendlines.append(line)
      plt.legend(handles=legendlines, loc=1)
      #plt.axis([0, 6, 0, 20])

    plt.savefig('%s.png' % name)
    plt.close()

p = PerfReport()
p.create_report('map_time')
p.create_report('avg_time')
p.create_report('shift_time')
