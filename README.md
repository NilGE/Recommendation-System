# Recommendation-System


##Import Json Data
```
hadoop fs -mkdir -p project/src_data/business
hadoop fs -mkdir -p project/src_data/checkin
hadoop fs -mkdir -p project/src_data/user
hadoop fs -mkdir -p project/src_data/review
hadoop fs -mkdir -p project/src_data/business_sample

cd ~/source_data/json_data/

hadoop fs -put yelp_training_set_business.json project/src_data/business
hadoop fs -put yelp_training_set_checkin.json project/src_data/checkin
hadoop fs -put yelp_training_set_user.json project/src_data/user
hadoop fs -put yelp_training_set_review.json project/src_data/review
hadoop fs -put business.json project/src_data/business_sample
```
