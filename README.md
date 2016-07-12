# Recommendation-System

```
hadoop fs -mkdir -p project/src_data/business
hadoop fs -mkdir -p project/src_data/checkin
hadoop fs -mkdir -p project/src_data/user
hadoop fs -mkdir -p project/src_data/review

hadoop fs -put /home/1604xiangyu/source_data/json_data/yelp_training_set_business.json project/src_data/business
hadoop fs -put /home/1604xiangyu/source_data/json_data/yelp_training_set_checkin.json project/src_data/checkin
hadoop fs -put /home/1604xiangyu/source_data/json_data/yelp_training_set_user.json project/src_data/user
hadoop fs -put /home/1604xiangyu/source_data/json_data/yelp_training_set_review.json project/src_data/review
```
