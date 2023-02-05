from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import month, desc, to_date, when, struct, avg, lit, max, date_format, hour, rank, dayofweek
from functools import reduce
import numpy as np
import time
from IPython.display import display
import warnings
warnings.filterwarnings('ignore')


spark = SparkSession.builder.appName("AdvancedDB_Project")\
							.master("spark://192.168.0.1:7077").getOrCreate()


def print_dataframe(df, n, title) :
  print(title)
  df = df.limit(n).toPandas()
  df.index = np.arange(1, len(df) + 1)
  display(df)

# read dataset
# df1: .parquet files 
# df2: .csv file
df1 = spark.read.option("header", "true").format("parquet").load('hdfs://master:9000/Dataset/*.parquet')
df2 = spark.read.option("header", "true").format("csv").load('hdfs://master:9000/Dataset/taxi+_zone_lookup.csv')

# preprocessing: remove from df1 records not having dates between January 2022 - June 2022
df1 = df1.filter(to_date(df1["tpep_pickup_datetime"]).between("2022-01-01", "2022-06-30"))

# create rdds from dataframes
rdd1 = df1.rdd
rdd2 = df2.rdd

# a useful extension of df1 in order to include month column
df1_month = df1.withColumn("month", month(df1["tpep_pickup_datetime"]))

# Query Q1
# (1) Keep March records only
# (2) Match Drop Off Location ID's with Zone names
# (3) Keep records with "Battery Park" as Drop off Zone 
# (4) Find max tip amount
# (5) Choose all records with tip amount equal to max tip amount  

start_time = time.time()

df_Q1 = df1.filter(to_date(df1["tpep_pickup_datetime"]).between("2022-03-01", "2022-03-31")) 
df_Q1 = df_Q1.join(df2, df_Q1["DOLocationID"]==df2["LocationID"])
df_Q1 = df_Q1.filter(df_Q1["Zone"]=="Battery Park")

max_tip = df_Q1.agg(max("tip_amount").alias("max_tip")).first()["max_tip"]
df_Q1 = df_Q1.filter(df_Q1["tip_amount"] == max_tip)

result = df_Q1.collect()
elapsed_time = time.time() - start_time

print_dataframe(df_Q1, df_Q1.count(), "Query Q1")
print(f"Elapsed time: {elapsed_time:.2f} sec")
print("-"*100)


# Query Q2
# (1) Find max tolls amounts for each month 
# (2) Find all records with max tolls amounts for each month 
# (3) Delete extra columns "tolls_amount" and "month"
# (4) Sort final records by month (January -> June)

start_time = time.time()

df_Q2 = df1_month.groupBy(df1_month["month"].alias("month_num")).agg(max("tolls_amount").alias("max_tolls_amount"))
df_Q2 = df_Q2.join(df1_month, (df_Q2["month_num"] == df1_month["month"]) & (df_Q2["max_tolls_amount"] == df1_month["tolls_amount"]))
df_Q2 = df_Q2.drop("tolls_amount", "month")
df_Q2 = df_Q2.orderBy("month_num")

month_mapping = {1:"January", 2:"February", 3:"March", 4:"April", 5:"May", 6:"June"}
month_mapping_conditions = [(df_Q2["month_num"] == i, month_mapping[i]) for i in range(1, 7)]
df_Q2 = df_Q2.withColumn("month_num", reduce(lambda acc, condition : when(condition[0], condition[1]).otherwise(acc), month_mapping_conditions, lit(None)))\
              .withColumnRenamed("month_num", "month")

result = df_Q2.collect()
elapsed_time = time.time() - start_time

print_dataframe(df_Q2, df_Q2.count(), "Query Q2")
print(f"Elapsed time: {elapsed_time:.2f} sec")
print("-"*100)


# Query Q3 (dataframe)
# (1) Group records by fortnight (15-day interval) they belong 
# (2) Keep records with different pickup and dropoff area
# (3) Calculate average trip distance and cost for records in the same fortnight
# (4) Sort records by fortnight (January -> June)

start_time = time.time()

dates = sorted([("2022-0"+str(i)+"-01", "2022-0"+str(i)+"-15") for i in range(1,7)] + \
        [("2022-02-16", "2022-02-28")] + \
        [("2022-0"+str(i)+"-16", "2022-0"+str(i)+"-31") for i in range(1,7,2)] + \
        [("2022-0"+str(i)+"-16", "2022-0"+str(i)+"-30") for i in range(4,7,2)])

date_conditions = [(to_date(df1["tpep_pickup_datetime"]).between(dates[i][0], dates[i][1]), "[" + dates[i][0] + ", " +  dates[i][1] + "]") for i in range(12)]

df_Q3 = df1.withColumn("fortnight", reduce(lambda acc, condition : when(condition[0], condition[1]).otherwise(acc), date_conditions, lit(None)))
df_Q3 = df_Q3.filter(df_Q3["PULocationID"] != df_Q3["DOLocationID"])
df_Q3 = df_Q3.groupby(df_Q3["fortnight"]).agg(avg("trip_distance").alias("avg distance"), avg("total_amount").alias("avg charge"))
df_Q3 = df_Q3.orderBy("fortnight")

result = df_Q3.collect()
elapsed_time = time.time() - start_time

print_dataframe(df_Q3, df_Q3.count(), "Query Q3 (with Dataframe)")
print(f"Elapsed time: {elapsed_time:.2f} sec")
print("-"*100)


# Query Q3 (rdd)
def check_date(row) :
  date = row.tpep_pickup_datetime.date().strftime("%Y-%m-%d")
  for i in range(12) :
    if date >= dates[i][0] and date <= dates[i][1] :
      return ("[" + dates[i][0] + ", " +  dates[i][1] + "]", (row.trip_distance, row.total_amount, 1))


start_time = time.time()

rdd_Q3 = rdd1.filter(lambda x : x.PULocationID != x.DOLocationID).map(check_date)\
              .reduceByKey(lambda a,b:(a[0]+b[0], a[1]+b[1], a[2]+b[2]))\
              .mapValues(lambda a : (a[0]/a[2], a[1]/a[2]))\
              .sortBy(lambda a : a[0])\
              .map(lambda a : (a[0], a[1][0], a[1][1]))


result = rdd_Q3.collect()
elapsed_time = time.time() - start_time

df_Q3_from_rdd = rdd_Q3.toDF(["fortnight", "avg distance", "avg charge"])

print_dataframe(df_Q3_from_rdd, df_Q3_from_rdd.count(), "Query Q3 (with RDD)")
print(f"Elapsed time: {elapsed_time:.2f} sec")
print("-"*100)


# Query Q4
# (1) Group records by day and hour interval they belong
# (2) Calculate number-of-passengers/hour/day
# (3) Keep top-3 hours/day with most passengers in a taxi ride

start_time = time.time()

hour_intervals = [(i, i+1) for i in range(24)]

hour_conditions = [((hour(df1["tpep_pickup_datetime"]) >= hour_intervals[i][0]) &
                    (hour(df1["tpep_pickup_datetime"]) < hour_intervals[i][1]), f"[{i}, {(i+1)%24})") for i in range(24)]

df_Q4 = df1.withColumn("day", dayofweek(df1["tpep_pickup_datetime"]))\
            .withColumn("hour_interval", reduce(lambda acc, condition : when(condition[0], condition[1]).otherwise(acc), hour_conditions, lit(None)))

df_Q4 = df_Q4.groupby(df_Q4["day"], df_Q4["hour_interval"]).agg(avg(df_Q4["passenger_count"]).alias("number_passengers/hour/day"))

window = Window.partitionBy("day").orderBy(desc("number_passengers/hour/day"))
df_Q4 = df_Q4.withColumn("rank", rank().over(window))

df_Q4 = df_Q4.filter(df_Q4["rank"] <= 3)

df_Q4 = df_Q4.orderBy([df_Q4["day"], df_Q4["rank"]])

day_mapping = {1:"Monday", 2:"Tuesday", 3:"Wednesday", 4:"Thursday", 5:"Friday", 6:"Saturday", 7:"Sunday"}
day_mapping_conditions = [(df_Q4["day"] == i, day_mapping[i]) for i in range(1, 8)]
df_Q4 = df_Q4.withColumn("day", reduce(lambda acc, condition : when(condition[0], condition[1]).otherwise(acc), day_mapping_conditions, lit(None)))\
              .drop(df_Q4["rank"])

result = df_Q4.collect()
elapsed_time = time.time() - start_time

print_dataframe(df_Q4, df_Q4.count(), "Query Q4")
print(f"Elapsed time: {elapsed_time:.2f} sec")
print("-"*100)


# Query Q5
# (1) Find tip/fare percentage for each record
# (2) Caculate max tip/fare percentage by date by month
# (3) Keep top-5 dates/month with biggest tip/fare percentage

start_time = time.time()

df_Q5 = df1_month.withColumn("% tip/fare", df1["tip_amount"]/df1["fare_amount"]*100)\
            .withColumn("date", to_date(df1["tpep_pickup_datetime"]))

df_Q5 = df_Q5.groupby([df_Q5["month"], df_Q5["date"]]).agg(max(df_Q5["% tip/fare"]).alias("max % tip/fare"))

window = Window.partitionBy("month").orderBy(desc("max % tip/fare"))
df_Q5 = df_Q5.withColumn("rank", rank().over(window))

df_Q5 = df_Q5.filter(df_Q5["rank"] <= 5)

df_Q5 = df_Q5.orderBy([df_Q5["month"], df_Q5["rank"]])

month_mapping = {1:"January", 2:"February", 3:"March", 4:"April", 5:"May", 6:"June"}
month_mapping_conditions = [(df_Q5["month"] == i, month_mapping[i]) for i in range(1, 7)]
df_Q5 = df_Q5.withColumn("month", reduce(lambda acc, condition : when(condition[0], condition[1]).otherwise(acc), month_mapping_conditions, lit(None)))\
              .drop(df_Q5["rank"])

result = df_Q5.collect()
elapsed_time = time.time() - start_time

print_dataframe(df_Q5, df_Q5.count(), "Query Q5")
print(f"Elapsed time: {elapsed_time:.2f} sec")