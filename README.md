# hadoop join and aggregate 

## Introduction
* Joined and aggregated two dataset with two method (a multiple MapReduce job and a single MapReduce job) in hadoop  
* query4 used reducer side join. This method needs one map-reduce task to join 2 dataset and write intermidate results to HDFS, and another map-reduce task read back the intermidate result to do aggregation. 
* query4.1 used mapper side join. This method only needs one map-reduce task to complete. It uses distributed cache to do map side join and in reduce phase just do the aggregation.   
