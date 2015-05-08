# hadoop join and aggregate 

## Introduction
* Join and aggregat two dataset with two different methods in hadoop.   
* query4 needs two map-reduce tasks to complete. This method applies reducer side join. It needs one map-reduce task to join 2 dataset, writes intermidate results to HDFS, and another map-reduce task reads back the intermidate result to do aggregation. 
* query4.1 needs only one map-reduce task to complete. This method applies mapper side join. It uses distributed cache to do map side join and in reduce phase just do the aggregation.   

