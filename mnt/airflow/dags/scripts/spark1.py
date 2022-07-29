import os
import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType

# Define SparkApp params

appName = "yelp-dataset-with-spark"

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName(appName) \
    .enableHiveSupport() \
    .getOrCreate()


# Define HDFS paths
staging_yelp_data_path = 'hdfs://namenode:9000/staging/yelp-dataset/yelp-dataset/'
raw_yelp_data_path = 'hdfs://namenode:9000/raw/yelp-dataset/yelp-dataset/'
business_file_path = os.path.join(raw_yelp_data_path, "yelp_academic_dataset_business.json")
checkin_file_path = os.path.join(raw_yelp_data_path, "yelp_academic_dataset_checkin.json")
review_file_path = os.path.join(raw_yelp_data_path, "yelp_academic_dataset_review.json")
tip_file_path = os.path.join(raw_yelp_data_path, "yelp_academic_dataset_tip.json")
user_file_path = os.path.join(raw_yelp_data_path, "yelp_academic_dataset_user.json")

# Read the file forex_rates.json from the HDFS
business_df = spark.read.json(business_file_path)
checkin_df = spark.read.json(checkin_file_path)
review_df = spark.read.json(review_file_path)
tip_df = spark.read.json(tip_file_path)
user_df = spark.read.json(user_file_path)

# *****************************
# implement the business logic
# *****************************

# Schemas
# ********************************************
# review
#  |-- business_id: string (nullable = true)
#  |-- cool: long (nullable = true)
#  |-- date: string (nullable = true)
#  |-- funny: long (nullable = true)
#  |-- review_id: string (nullable = true)
#  |-- stars: double (nullable = true)
#  |-- text: string (nullable = true)
#  |-- useful: long (nullable = true)
#  |-- user_id: string (nullable = true)

# business
#  |-- address: string (nullable = true)
#  |-- attributes: struct (nullable = true)
#  |    |-- AcceptsInsurance: string (nullable = true)
#  |    |-- AgesAllowed: string (nullable = true)
#  |    |-- Alcohol: string (nullable = true)
#  |    |-- Ambience: string (nullable = true)
#  |    |-- BYOB: string (nullable = true)
#  |    |-- BYOBCorkage: string (nullable = true)
#  |    |-- BestNights: string (nullable = true)
#  |    |-- BikeParking: string (nullable = true)
#  |    |-- BusinessAcceptsBitcoin: string (nullable = true)
#  |    |-- BusinessAcceptsCreditCards: string (nullable = true)
#  |    |-- BusinessParking: string (nullable = true)
#  |    |-- ByAppointmentOnly: string (nullable = true)
#  |    |-- Caters: string (nullable = true)
#  |    |-- CoatCheck: string (nullable = true)
#  |    |-- Corkage: string (nullable = true)
#  |    |-- DietaryRestrictions: string (nullable = true)
#  |    |-- DogsAllowed: string (nullable = true)
#  |    |-- DriveThru: string (nullable = true)
#  |    |-- GoodForDancing: string (nullable = true)
#  |    |-- GoodForKids: string (nullable = true)
#  |    |-- GoodForMeal: string (nullable = true)
#  |    |-- HairSpecializesIn: string (nullable = true)
#  |    |-- HappyHour: string (nullable = true)
#  |    |-- HasTV: string (nullable = true)
#  |    |-- Music: string (nullable = true)
#  |    |-- NoiseLevel: string (nullable = true)
#  |    |-- Open24Hours: string (nullable = true)
#  |    |-- OutdoorSeating: string (nullable = true)
#  |    |-- RestaurantsAttire: string (nullable = true)
#  |    |-- RestaurantsCounterService: string (nullable = true)
#  |    |-- RestaurantsDelivery: string (nullable = true)
#  |    |-- RestaurantsGoodForGroups: string (nullable = true)
#  |    |-- RestaurantsPriceRange2: string (nullable = true)
#  |    |-- RestaurantsReservations: string (nullable = true)
#  |    |-- RestaurantsTableService: string (nullable = true)
#  |    |-- RestaurantsTakeOut: string (nullable = true)
#  |    |-- Smoking: string (nullable = true)
#  |    |-- WheelchairAccessible: string (nullable = true)
#  |    |-- WiFi: string (nullable = true)
#  |-- business_id: string (nullable = true)
#  |-- categories: string (nullable = true)
#  |-- city: string (nullable = true)
#  |-- hours: struct (nullable = true)
#  |    |-- Friday: string (nullable = true)
#  |    |-- Monday: string (nullable = true)
#  |    |-- Saturday: string (nullable = true)
#  |    |-- Sunday: string (nullable = true)
#  |    |-- Thursday: string (nullable = true)
#  |    |-- Tuesday: string (nullable = true)
#  |    |-- Wednesday: string (nullable = true)
#  |-- is_open: long (nullable = true)
#  |-- latitude: double (nullable = true)
#  |-- longitude: double (nullable = true)
#  |-- name: string (nullable = true)
#  |-- postal_code: string (nullable = true)
#  |-- review_count: long (nullable = true)
#  |-- business_stars: double (nullable = true)
#  |-- state: string (nullable = true)

# UDFs
# ********************************************
todate =  F.udf(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"), DateType())
# ********************************************

# REVIEWS
# ********************************************
# review parse date
review_df = review_df.select("stars", "business_id", "user_id", "text", "useful", "cool", "date")
review_df = review_df.withColumn("date", todate(F.col("date")))
review_df = review_df.withColumn('weekday', F.dayofmonth(F.col("date")))\
    .withColumn('calendarweek', F.weekofyear(F.col("date")))\
    .withColumn('month', F.month(F.col("date")))\
    .withColumn('year', F.year(F.col("date")))


review_df.select("stars", "business_id", "user_id", "text", "useful", "cool",
                 "date", "weekday", "calendarweek", "month", "year")\
    .write.mode("overwrite").format('hive').saveAsTable('review')

# business_avg_stars_calendarweek
business_avg_stars_calendarweek = review_df.select(["business_id", "calendarweek", "year", "stars"])\
    .groupBy("business_id", "calendarweek", "year").agg(F.mean("stars"))\
    .write.mode("overwrite").format('hive').saveAsTable('business_avg_stars_calendarweek')

# business_avg_stars_month
business_avg_stars_month = review_df.select(["business_id", "month", "stars"])\
    .groupBy("business_id", "month").agg(F.mean("stars"))\
    .write.mode("overwrite").format('hive').saveAsTable('business_avg_stars_month')


# BUSINESS
# ********************************************
business_df.select("address","attributes","business_id",
                   "categories","city","hours","is_open","latitude","longitude",
                   "name","postal_code","review_count","state")\
    .write.mode("overwrite").format('hive').saveAsTable('business')


# df_business_join_avgStars
df_avg_review_stars = review_df.groupBy('business_id').agg(F.mean('stars'))
df_business_join_avgStars = business_df.join(df_avg_review_stars, on=['business_id'], how='inner')
df_business_join_avgStars.select('avg(stars)','stars','name','city','state')\
    .write.mode("overwrite").format('hive').saveAsTable('business_join_avgStars')


# now see top most reviewed business.
# so take review data which has rating(stars) more than 3
review_star_three = review_df.filter('stars >3')
grouped_review = review_star_three.groupby('business_id').count()
review_sort = grouped_review.sort('count',ascending=False)

# Top 10 category which has most business count
from pyspark.sql.functions import split,explode
category = business_df.select('categories')
individual_category = category.select(explode(split('categories', ',')).alias('category'))
grouped_category = individual_category.groupby('category').count()
top_category = grouped_category.sort('count',ascending=False)
top_category.write.mode('overwrite').csv(os.path.join(staging_yelp_data_path, "top_category.csv"))

# Top Rating give by User to business
rating = business_df.select('stars')
group_rating = rating.groupby('stars').count()
rating_top = group_rating.sort('count',ascending=False)
rating_top.write.mode('overwrite').csv(os.path.join(staging_yelp_data_path, "top_rating.csv"))

# Top Locations who have number of business more in world
locations = business_df.select('business_id','city')
review_city = review_df.select('business_id')
merge_city = locations.join(review_city,'business_id','inner')
grouped_review_city = merge_city.groupby('city').count()
most_reviewed_city = grouped_review_city.groupby('city').sum()
most_reviewed_city.sort('sum(count)',ascending=False)\
    .write.mode('overwrite').csv(os.path.join(staging_yelp_data_path, "most_reviewed_city.csv"))