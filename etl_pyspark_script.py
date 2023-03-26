from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, current_timestamp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from datetime import date
import sys 
import json

# parse the file names of each of the files in S3
file_paths = json.loads(sys.argv[1])

spark = SparkSession.builder.getOrCreate()

current_date = date.today()
# define file locations
input_dir = 's3://dwcd-midterm/'
inventory_file = input_dir + file_paths['inventory']
calendar_file = input_dir + file_paths['calendar']
product_file = input_dir + file_paths['product']
sales_file = input_dir + file_paths['sales']
store_file = input_dir + file_paths['store']
results_dir = 's3://dwcd-midterm/analysis/'
output_agg_tbl = 'weekly_agg_table'
output_calendar_tbl = 'calendar_table'
output_store_tbl = 'store_table'
output_prod_tbl = 'prod_table'

# START: read original CSV files
sales = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(sales_file)

inventory = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(inventory_file)

calendar = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(calendar_file)

store = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(store_file)

product = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(product_file)
# END: read original CSV files

sales = sales.withColumn('TRANS_DT', to_date(col('TRANS_DT')))
sales.createOrReplaceTempView("tbl_sales")
# creating aggregated sales table
df_sum_sales_qty = spark.sql(
    """SELECT 
            TRANS_DT,
            STORE_KEY,
            PROD_KEY,
            sum(SALES_QTY) as total_sales_qty,
            sum(SALES_AMT) as total_sales_amount,
            avg(SALES_PRICE) as avg_sales_price,
            sum(SALES_COST + SHIP_COST) as total_cost
        FROM tbl_sales
        GROUP BY 
            TRANS_DT,
            STORE_KEY,
            PROD_KEY"""
)

# create slice of calendar table (cal) for joining into the fact tables (week #)
calendar = calendar.withColumn('CAL_DT', col('CAL_DT').cast(DateType()))
cal = calendar.withColumnRenamed('CAL_DT', 'Date')

sales_aggr = df_sum_sales_qty.join(cal, df_sum_sales_qty.TRANS_DT == cal.Date, 'left')

inventory = inventory.withColumn('CAL_DT', col('CAL_DT').cast(DateType())) \
    .withColumn('NEXT_DELIVERY_DT', col('NEXT_DELIVERY_DT').cast(DateType()))

inventory_eow = inventory.join(cal, inventory.CAL_DT == cal.Date, 'left')

cal = cal.select('Date', 'DAY_OF_WK_NUM', 'YR_WK_NUM')

# NOTE the inventory table is incomplete so it will not have info for every Saturday 
# therefore a new dataset which establishes the last day for each week of data is needed --> inv_max_day

inv_max_day = inventory_eow.groupBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY").agg(F.max("DAY_OF_WK_NUM").alias("max_day_of_week"))
inv_max_day = inv_max_day.withColumnRenamed("YR_WK_NUM", "_YR_WK_NUM") \
    .withColumnRenamed("STORE_KEY", "_STORE_KEY") \
    .withColumnRenamed("PROD_KEY", "_PROD_KEY") 

# inner join with main inventory weekly tbl to filter each week + prod + store to the max day
inventory_eow = inventory_eow.join(
    inv_max_day, 
    (inventory_eow.YR_WK_NUM == inv_max_day._YR_WK_NUM)
    & (inventory_eow.STORE_KEY == inv_max_day._STORE_KEY)
    & (inventory_eow.PROD_KEY == inv_max_day._PROD_KEY),
    how='inner'
)
# getting rid of unnecessary cols and renaming for joins
inventory_eow = inventory_eow \
    .withColumnRenamed('INVENTORY_ON_HAND_QTY', 'stock_lvl_eow') \
    .withColumnRenamed('INVENTORY_ON_ORDER_QTY', 'stock_on_order_eow') \
    .drop('CAL_TYPE_DESC',
          'MNTH_NUM',
          'QTR_NUM',
          'YR_QTR_NUM',
          'YR_MNTH_NUM',
          'Date',
          'OUT_OF_STOCK_FLG',
          'WASTE_QTY',
          'PROMOTION_FLG',
          'NEXT_DELIVERY_DT',
          "CAL_DT",
          "STORE_KEY",
          "PROD_KEY",
          "DAY_OF_WK_NUM",
          "YR_WK_NUM",
          "max_day_of_week"
          )

# creating another view of the inventory data to aggregate the entire week (not only last day)
inv_forjoin = inventory \
    .withColumnRenamed("PROD_KEY", "_PROD_KEY") \
    .withColumnRenamed("STORE_KEY", "_STORE_KEY") \
    .withColumnRenamed("CAL_DT", "_CAL_DT")

inv_forjoin = inv_forjoin.join(cal, inv_forjoin._CAL_DT == cal.Date)
inv_forjoin = inv_forjoin.groupBy("YR_WK_NUM", "_STORE_KEY", "_PROD_KEY") \
    .agg(F.sum("OUT_OF_STOCK_FLG").alias("No_Stock_Instances")) \
    .withColumnRenamed("YR_WK_NUM", "_YR_WK_NUM")
# aggregate the slice of the sales tbl to week + prod + store 
sales_aggr = sales_aggr \
    .groupBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY") \
    .agg(F.sum("total_sales_qty").alias("total_sales_qty"), 
         F.sum("total_sales_amount").alias("total_sales_amount"),
         F.sum("total_cost").alias("total_cost")
         )
# join with aggregated inventory table
agg_df = sales_aggr.join(inv_forjoin, 
                        (sales_aggr.PROD_KEY == inv_forjoin._PROD_KEY) 
                        & (sales_aggr.STORE_KEY == inv_forjoin._STORE_KEY)
                        & (sales_aggr.YR_WK_NUM == inv_forjoin._YR_WK_NUM),
                        how='inner'
                         )

agg_df2 = agg_df.drop('_PROD_KEY', '_STORE_KEY', '_YR_WK_NUM')

agg_df3 = agg_df2.join(
    inventory_eow,
    (agg_df2.YR_WK_NUM == inventory_eow._YR_WK_NUM)
    & (agg_df2.STORE_KEY == inventory_eow._STORE_KEY)
    & (agg_df2.PROD_KEY == inventory_eow._PROD_KEY)
)

agg_df4 = agg_df3.drop("_YR_WK_NUM", "_STORE_KEY", "_PROD_KEY")
# now adding calculated columns:
agg_df5 = agg_df4.withColumn("Low_Stock_flg", F.when(agg_df4.stock_lvl_eow < agg_df4.total_sales_qty, 1).otherwise(0))

agg_df6 = agg_df5.withColumn("Total_Low_Stock_Impact", col("No_Stock_Instances") + col("Low_Stock_flg"))

agg_df7 = agg_df6.withColumn("avg_sales_price", col("total_sales_amount") / col("total_sales_qty"))

agg_df8 = agg_df7 \
    .withColumn("Pot_Low_Stock_Impact", 
                F.when(agg_df7.Low_Stock_flg == 1, 
                       (col("total_sales_amount") - col("stock_lvl_eow")*col("avg_sales_price"))).otherwise(0)) \
    .withColumn("No_Stock_Impact", 
                F.when(agg_df7.No_Stock_Instances > 0, 
                       agg_df7.total_sales_amount).otherwise(0)) \
    .withColumn("in_stock_pct",
                (1 - col("No_Stock_Instances") / 7) * 100
                ) \
    .withColumn("weeks_supply",
                col("stock_lvl_eow") / col("total_sales_qty")
                )

agg_df9 = agg_df8.drop("DAY_OF_WK_DESC", "YR_NUM", "WK_NUM")

# add a filtering step to exclude the future
current_week_df = calendar.filter(calendar.CAL_DT == current_date).select('CAL_DT', 'YR_WK_NUM')
current_week = current_week_df.select(col('YR_WK_NUM')).first()[0]

# adding timestamp and filtering
agg_df10 = agg_df9.withColumn("load_tmst", current_timestamp()) \
    .filter(agg_df9.YR_WK_NUM <= current_week)
calendar = calendar.withColumn("load_tmst", current_timestamp())
store = store.withColumn("load_tmst", current_timestamp())
product = product.withColumn("load_tmst", current_timestamp())

# Writing results
agg_df10.repartition(1) \
    .write \
    .mode("overwrite") \
    .parquet(results_dir + output_agg_tbl)

calendar.repartition(1) \
    .write \
    .mode("overwrite") \
    .parquet(results_dir + output_calendar_tbl)

store.repartition(1) \
    .write \
    .mode("overwrite") \
    .parquet(results_dir + output_store_tbl)

product.repartition(1) \
    .write \
    .mode("overwrite") \
    .parquet(results_dir + output_prod_tbl)
