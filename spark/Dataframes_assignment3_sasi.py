from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, date_format, col, to_timestamp, hour, unix_timestamp, lag
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("DataFrameLoader").getOrCreate()

# Load data from the text file
input_path = "/home/sasi/PycharmProjects/pythonProject/spark_assignmnt.txt"
df = spark.read.option("header", "true").csv(input_path)

# Clean up and format columns
df = df.withColumn("title", trim(col("title"))) \
       .withColumn("name", trim(col("name"))) \
       .withColumn("persistentCart", trim(col("persistentCart"))) \
       .withColumn("timestamp", date_format(col("timestamp"), "MM/dd/yyyy HH:mm:ss"))

df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "MM/dd/yyyy HH:mm:ss"))

# Add Sys_date column
df = df.withColumn("Sys_date", date_format(col("timestamp"), "MM/dd/yyyy"))

# Define the output path
output_path = "/home/sasi/PycharmProjects/pythonProject/output_parquet"

# Write DataFrame to Parquet format
df.write.mode("overwrite").parquet(output_path)

# Load the Parquet file into a new DataFrame
df_parquet = spark.read.parquet(output_path)

# Total visits per title
visits_per_title = df_parquet.groupBy("title").count().alias("total_visits")
visits_per_title.show(truncate=False)

# Most active hour
hour_visits = df_parquet.withColumn("hour", hour("timestamp")) \
    .groupBy("hour").count().orderBy("count", ascending=False)
hour_visits.show(truncate=False)

# Top user with the most visits
user_visits = df_parquet.groupBy("name").count().orderBy("count", ascending=False)
user_visits.show(1, truncate=False)

# Top user for "Remote Support: Geek Squad - Best Buy"
remote_support_visits = df_parquet.filter(df_parquet.title == 'Remote Support: Geek Squad - Best Buy') \
    .groupBy("name").count().orderBy("count", ascending=False)
remote_support_visits.show(1, truncate=False)

# Users common to "Best Buy Support & Customer Service" and "Remote Support: Geek Squad - Best Buy"
best_buy_support_users = df_parquet.filter(df_parquet.title == 'Best Buy Support & Customer Service') \
    .select("name").distinct()
remote_support_users = df_parquet.filter(df_parquet.title == 'Remote Support: Geek Squad - Best Buy') \
    .select("name").distinct()
common_users = best_buy_support_users.intersect(remote_support_users)
common_users_count = common_users.count()
print(f"Number of users with both titles: {common_users_count}")

# Users common to "Best Buy Support & Customer Service" and "Schedule a Service - Best Buy"
schedule_service_users = df_parquet.filter(df_parquet.title == 'Schedule a Service - Best Buy') \
    .select("name").distinct()
common_users_schedule = best_buy_support_users.intersect(schedule_service_users)
common_users_schedule_count = common_users_schedule.count()
print(f"Number of users with both titles: {common_users_schedule_count}")

# Time difference analysis
window_spec = Window.partitionBy("name").orderBy("timestamp")
df_with_lag = df_parquet.withColumn("prev_time", lag("timestamp").over(window_spec)) \
    .withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_time"))

# User with the longest time interval between visits
longest_interval_user = df_with_lag.orderBy("time_diff", ascending=False).select("name", "time_diff").limit(1)
longest_interval_user.show(truncate=False)

# User with the shortest time interval between visits
shortest_interval_user = df_with_lag.filter(col("time_diff").isNotNull()) \
    .orderBy("time_diff", ascending=True).select("name", "time_diff").limit(1)
shortest_interval_user.show(truncate=False)


