.mode tabs
.header on

select (nodes*blocks*block_size)/1024 as gb,
       (nodes*cores*nparts) as nparts,
       nodes,blocks,block_size as blk_sz,
       map_time/1E9 as rdd,
       shift_time/1E9 as rdd_shift,
       avg_time/1E9 as rdd_avg
       from results
       order by nodes,cores,blocks,block_size;
