.mode tabs
.header on

select (nodes*blocks*block_size)/1024 as sum_gb,
       (nodes*cores*nparts) as partitions,
       nodes,blocks,block_size,
       map_time/1E9 as map_time_sec,
       shift_time/1E9 as shift_time_sec
       from results
       order by nodes,cores,blocks,block_size;
