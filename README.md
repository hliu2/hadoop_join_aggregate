# hadoop join and aggregate 

## Introduction
* Join and aggregat two dataset with two different methods in hadoop.   
* Query4.java uses two map-reduce tasks to complete. This method applies reducer side join. It needs one map-reduce task to join 2 dataset, writes intermidate results to HDFS, and another map-reduce task reads back the intermidate result to do aggregation. 
* Query4_1.java uses only one map-reduce task to complete. This method applies mapper side join. It uses distributed cache to do map side join and conducts the aggregation in reduce phase.
* Map side join is faster than reducer side join. But map side join is adequate only when one of the tables on which you perform map-side join operation is small enough to fit into the memory. 

## Dateset Information
* Customer dataset: each line contains: CustID, Name, Age, CountryCode, Salary.   
* Transactions dataset: each line contains: TransID, CustID, TransTotal, TransNumItems, TransDesc.
* reports for every country code the number of customers having this code as well as the min and max of TransTotal fields for the transactions done by those customers. 

