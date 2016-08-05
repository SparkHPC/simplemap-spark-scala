#!/usr/bin/env python2

# Parse XML performance data and write to an sqlite3 database for reporting.

import os
import os.path
import sys
import sqlite3
import json


class Results(object):
   def __init__(self, filename):
      self.filename = filename
      with open(filename) as f:
        self.json = json.load(f)
      self.results = self.json['results']
      self.experiment = self.results['experiment']
      self.config = self.results['config']
      self.report = self.results['report']

   def job_id(self):
      path_info = os.path.split(self.filename)
      (basename, ext) = os.path.splitext(path_info[1])
      return basename

   def experiment_id(self):
      return self.experiment.get('id', '')

   def report_map_time(self):
      return float(self.report.get('mapTime', 0))

   def report_shift_time(self):
      return float(self.report.get('shiftTime', 0))

   def config_src(self):
      return self.config.get('src', 0)

   def config_dst(self):
      return self.config.get('dst', 0)

   def config_cores(self):
      return int(self.config.get('cores', 0))

   def config_generate(self):
      if self.config.get('generate', 'false') == 'true':
         return 1
      else:
         return 0

   def config_blocks(self):
      return int(self.config.get('blocks', 0))

   def config_block_size(self):
      return int(self.config.get('blockSize', 0))

   def config_nparts(self):
      return int(self.config.get('nparts', 0))

   def config_size(self):
      return int(self.config.get('size', 0))

   def config_nodes(self):
      return int(self.config.get('nodes', 0))

      
DROP_RESULTS_TABLE="DROP TABLE IF EXISTS results"

CREATE_RESULTS_TABLE="""CREATE TABLE results
  (id integer primary key,
   experiment_id integer,
   generate integer,
   nodes integer,
   cores integer,
   nparts integer,
   blocks integer,
   block_size integer,
   map_time real,
   shift_time real,
   job_id text,
   src text,
   dst text)
"""

INSERT_RESULT="INSERT INTO results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

def next_id():
   id = 0
   while True:
     yield id
     id = id + 1

def write_results_db():
   conn = sqlite3.connect('simplemap-spark-scala.db')
   cursor = conn.cursor()
   cursor.execute(DROP_RESULTS_TABLE)
   cursor.execute(CREATE_RESULTS_TABLE)

   dir = sys.argv[1]
   results = []
   id = next_id()
   for file in os.listdir(dir):
      if file.endswith('.json'):
         path = os.path.join(dir, file)
         r = Results(path)
         results.append( (id.next(), r.experiment_id(), r.config_generate(), r.config_nodes(),
                          r.config_cores(), r.config_nparts(), r.config_blocks(), r.config_block_size(),
                          r.report_map_time(), r.report_shift_time(), r.job_id(), r.config_src(), r.config_dst()))


   cursor.executemany(INSERT_RESULT, results)
   conn.commit()
   conn.close()

if __name__ == '__main__':
   write_results_db()
