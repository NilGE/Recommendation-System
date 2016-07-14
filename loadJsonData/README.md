#README.md


```
hadoop fs -mkdir -p project/batch/loadJsonData/input/business_sample
hadoop fs -mkdir -p project/batch/loadJsonData/output/business_sample
hadoop fs -put ~/source_data/json_data/business.json project/batch/loadJsonData/input/business_sample
hadoop fs -ls project/batch/loadJsonData/input/business_sample
hadoop jar loadJsonData-0.0.2-SNAPSHOT.jar com.nilge.hadoop.loadJsonData.mapreduce.LoadJsonData project/batch/loadJsonData/input/business_sample project/batch/loadJsonData/output/business_sample
hadoop fs -cat project/batch/loadJsonData/output/business_sample/*
```
