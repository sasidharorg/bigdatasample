from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

out_path1 = "/home/sasi/PycharmProjects/pythonProject/member_dtl.txt"

df_m = spark.read.option("header", True).option("inferSchema", True).csv(out_path1)

#df_m.show()


out_path2 = "/home/sasi/PycharmProjects/pythonProject/df_sales_data_generator.txt"
df_s = spark.read.option("header", True).option("inferSchema", True).csv(out_path2)
#df_s.show()


out_path3 = "/home/sasi/PycharmProjects/pythonProject/webvistiornew.txt"
df_w = spark.read.option("header", True).option("inferSchema", True).csv(out_path3)
#df_w.show()

df_m.createOrReplaceTempView("member_dtl")
df_s.createOrReplaceTempView("sales_data")
df_w.createOrReplaceTempView("webvisitor")

query = """
WITH member AS (
    SELECT name, state, membership_status, timestamp FROM member_dtl
),
MBR_2 AS (
    SELECT *, row_number() OVER (PARTITION BY name ORDER BY timestamp DESC) AS rn FROM member
),
MBR_3 AS (
    SELECT name, state, membership_status, timestamp FROM MBR_2 WHERE rn = 1 AND UPPER(TRIM(membership_status)) = 'ACTIVE'
),
sales AS (
    SELECT *, row_number() OVER (PARTITION BY mem_name ORDER BY pur_timestamp DESC) AS rn FROM sales_data
),
sales2 AS (
    SELECT saleId, product, mem_name, price, pur_timestamp FROM sales WHERE rn = 1
),
mbr_sales AS (
    SELECT name, state, membership_status, timestamp, saleId, product, mem_name, price, pur_timestamp
    FROM MBR_3 
    INNER JOIN sales2  ON UPPER(TRIM(name)) = UPPER(TRIM(mem_name))
),
webvisitor as (
     select title,visitId,name,persistentCart,timestamp from webvisitor
),
webvisitor1 as (
     select *, row_number() over ( partition by name order by timestamp ) as rn from webvisitor
),
webvisitor2 as (
     select  title,visitId,name,persistentCart,timestamp from webvisitor1 where rn =1 
),
mem_sales_web as (
    select m.name m_name, state, membership_status, m.timestamp m_timestamp, 
    saleId, m.product, mem_name, m.price, pur_timestamp,
     title,visitId,w.name w_name,persistentCart,w.timestamp w_timestamp
    from 
    mbr_sales as m 
    join
    webvisitor2 w 
    on
    upper(trim(m.name))=upper(trim(w.name))
),
time_diff as (
    select *, unix_timestamp(pur_timestamp)-unix_timestamp(w_timestamp) as time_diff from mem_sales_web
)  
SELECT m_name,
CASE
    when time_diff <= 300 then 'Category1'
    when time_diff > 300 and time_diff < 1800 then 'category2'
    when time_diff > 1800  and time_diff < 3600 then 'category3'
    when time_diff > 3600 and time_diff < 14400 then 'category4'
    when time_diff > 14400 and time_diff < 28800 then 'category5'
    when time_diff > 28800 then 'category6'
    
    else 'no category'
END as category    
    
FROM time_diff;
"""

result_df = spark.sql(query)


result_df.show()
