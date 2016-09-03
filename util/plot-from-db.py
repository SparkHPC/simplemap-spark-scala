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

  def distinct_blocks_iterator(self):
    query = "select distinct(blocks) from results order by blocks"
    records = self.conn.execute(query)
    for r in records:
      yield r['blocks']

  def get_distinct_blocks(self):
    return list(self.distinct_blocks_iterator())

  def distinct_gb_iterator(self):
    query = "select distinct(blocks * block_size / 1024) as gb from results order by gb"
    records = self.conn.execute(query)
    for r in records:
      yield r['gb']
    
  def get_distinct_gb(self):
    return list(self.distinct_gb_iterator())

  def get_perf_by_blocks(self):
    query = """select * from results
               where blocks = %(blocks)d and nparts=4
               order by nodes"""

    by_blocks = {}
    for blocks in self.get_distinct_blocks():
	cursor = self.conn.execute(query % vars())
	by_blocks[blocks] = [
         { 'nodes' : result['nodes'],
           'map_time' : result['map_time'] / 1.0e9,
           'shift_time' : result['shift_time'] / 1.0e9,
           'avg_time' : result['shift_time'] / 1.0e9
         } for result in cursor ]
    return by_blocks

  def get_perf_by_gb(self):
    query = """select blocks * block_size / 1024 as gb, nodes, map_time, shift_time, avg_time from results
               where gb = %(gb)d and nparts=4
               order by nodes"""

    by_gb = {}
    for gb in self.get_distinct_gb():
        cursor = self.conn.execute(query % vars())
        by_gb[gb] = [
         { 'gb' : result['gb'], 
           'nodes' : result['nodes'],
           'map_time' : result['map_time'] / 1.0e9,
           'shift_time' : result['shift_time'] / 1.0e9,
           'avg_time' : result['shift_time'] / 1.0e9
         } for result in cursor ]
    return by_gb

  def create_weak_report(self, name):
    import matplotlib.pyplot as plt
    plt.xlabel('Nodes')
    plt.ylabel('Time (seconds)')
    plt.yscale('log')
    plt.title("Weak Scaling (%s)" % name)
    linestyle = linestyle_iterator()
    legendlines = []
    by_block = self.get_perf_by_blocks()
    for b in self.get_distinct_blocks():
      nodes = [ d['nodes'] for d in by_block[b]]
      t = [ d[name] for d in by_block[b]]
      line, = plt.plot(nodes, t, linestyle.next(), label="%(b)d MB" % vars(), linewidth=1)

      legendlines.append(line)
      plt.legend(handles=legendlines, loc=1)
      #plt.axis([0, 6, 0, 20])

    plt.savefig('weak_scaling_%s.png' % name)
    plt.close()

  def create_strong_report(self, name):
    import matplotlib.pyplot as plt
    plt.xlabel('Nodes')
    plt.ylabel('Time (seconds)')
    plt.yscale('log')
    plt.title("Strong Scaling (%s)" % name)
    linestyle = linestyle_iterator()
    legendlines = []
    by_gb = self.get_perf_by_gb()
    for b in self.get_distinct_gb():
      nodes = [ d['nodes'] for d in by_gb[b]]
      t = [ d[name] for d in by_gb[b]]
      line, = plt.plot(nodes, t, linestyle.next(), label="%(b)d MB" % vars(), linewidth=1)

      legendlines.append(line)
      plt.legend(handles=legendlines, loc=1)
      #plt.axis([0, 6, 0, 20])

    plt.savefig('strong_scaling_%s.png' % name)
    plt.close()

p = PerfReport()
p.create_weak_report('map_time')
p.create_weak_report('avg_time')
p.create_weak_report('shift_time')

p.create_strong_report('map_time')
p.create_strong_report('avg_time')
p.create_strong_report('shift_time')

