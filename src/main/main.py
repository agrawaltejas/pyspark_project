import os
import sys

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(ROOT_DIR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import round, col, expr, when, lag, lead
from pyspark.sql.window import Window
from utilities import helper
from utilities import logging
from utilities import analysis
import datetime


def main():

    spark = SparkSession.builder \
            .appName("My Spark Application") \
            .master("local") \
            .getOrCreate()

    logger = logging.Log4j(spark)

    ## Step1 : Reading/ingesting data from the source specified. In our case it is csv files in 'data' folder.

    '''
    If the source is kafka, we can use following properties of spark to ingest data : 
     df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic1") \
    .load()
    '''

    # Capturing start time of job
    start_time = datetime.datetime.now()
    logger.info(f"Data Ingestion start time : {start_time}")

    # Ingesting data from csv file paths.
    if not os.path.exists('data'):
        raise FileNotFoundError(f"Input path data not found.")

    try:
        # File path 1
        path1 = "data/customer.csv"
        customer_df = spark.read.option("header",True).option("inferSchema",True).csv(path1)
        logger.info(f"Successfully read data from file 1: {path1}")

        # File path 2
        path2 = "data/exchange_rate.csv"
        exchange_rate_df = spark.read.option("header",True).option("inferSchema",True).csv(path2)
        logger.info(f"Successfully read data from file 2: {path2}")

        # File path 3
        path3 = "data/transaction.csv"
        transaction_df = spark.read.option("header",True).option("inferSchema",True).csv(path3)
        logger.info(f"Successfully read data from file 3: {path3}")

    except Exception as e:
        logger.error(f"Error reading source data. {str(e)}")
        logger.error(f"Stopping pipeline as the source data is not captured.")
        spark.stop()

    # Step 2 : Data cleansing, validations :

    # 2.1 : Convert columns into proper data types.
    # InferSchema = true in the begining while fetching data from csv is taking care mostly for the data types.
    # Using helper file and it's functions from utilities.
    exchange_rate_df = helper.convert_data_types(exchange_rate_df, ['effective_date','rate'], ['date','double'])
    transaction_df = helper.convert_data_types(transaction_df, ['amount'], ['double'])

    # 2.2 : Deduplication of source data is necessary before persisting into data lake :
    customer_df = helper.dedup(customer_df, ["customer_id"], ["customer_id"])
    transaction_df = helper.dedup(transaction_df, ["transaction_id"], ["transaction_date"])
    exchange_rate_df = helper.dedup(exchange_rate_df, ["exchange_rate_id"], ["effective_date"])
    logger.info("Source Dataframes deduplicated")

    # After data cleansing , and validation through tests, we can persist the tables in data lake ( ex: GCS, Snowflake etc)
    # For now we will persist these tables in memory.
    customer_df.persist()
    exchange_rate_df.persist()
    transaction_df.persist()

    # Capturing metrics in logs :
    # Though we should not use much of actions in spark. Like count(), show() etc.
    logger.info(f"Metrics : Customer dataset count = {customer_df.count()} ")
    customer_df.printSchema()
    logger.info(f"Metrics : Exchange rate dataset count = {exchange_rate_df.count()} ")
    exchange_rate_df.printSchema()
    logger.info(f"Metrics : Transaction dataset count = {transaction_df.count()} ")
    transaction_df.printSchema()

    # Capturing total ingestion time :
    ingestion_end_time = datetime.datetime.now()
    logger.info(f"Source data successfully persisted. End time : {ingestion_end_time}" )
    ingestion_total_time = ingestion_end_time - start_time
    logger.info(f"Time taken in data ingestion : {ingestion_total_time}" )

    customer_df.createOrReplaceTempView("Customers")
    transaction_df.createOrReplaceTempView("Transactions")
    exchange_rate_df.createOrReplaceTempView("Exchange_rates")

    # Step 3: Merge Customers, Transactions and exchange tables to create final dataset :
    merged_df = transaction_df.join(customer_df, transaction_df.customer_id == customer_df.customer_id, "left").select(transaction_df['*'],customer_df['country'])

    # Transforming exchange_rates dataframe to get the final conversion in Euros for all records.
    eur_conversion_df = helper.currency_conversion(exchange_rate_df,'EUR')

    # Converting all currencies to a single EUR currency for further analysis:
    try :
        final_df = merged_df.join(eur_conversion_df, (merged_df.currency == eur_conversion_df.from_currency) & (merged_df.transaction_date == eur_conversion_df.effective_date),"left" )\
            .select(merged_df["*"],
                    when(merged_df['transaction_type'] == 'out', round(eur_conversion_df['rate']*(merged_df['amount'].cast('double'))*(-1), 3) )\
                    .otherwise( round(eur_conversion_df['rate']*(merged_df['amount'].cast('double')) , 3)  )\
                    .alias('amount_in_eur')\
                    )
        logger.info(f"Successfully converted currencies to same currency for analytical requirements")
    except Exception as e:
        logger.error(f"Error converting the currency in transactions : {str(e)}")
        spark.stop()

    # Adding valid_from, valid_to columns to determine transaction history in time.
    try :
        window_spec = Window.partitionBy('customer_id').orderBy('transaction_date')
        final_df = final_df.withColumn('valid_to', lead(col('transaction_date')).over(window_spec))
        final_df = final_df.withColumn('valid_from', lag(col('valid_to')).over(window_spec))
        final_df = final_df.withColumn('last_transaction', when(col('valid_to').isNull(), True).otherwise(False))
        logger.info(f"Successfully added valid_from and valid_to to determine the history of transactions for a user.")
    except Exception as e:
        logger.error(f"Error adding valid_from and valid_to columns : {str(e)}")
        spark.stop()

    # Selecting final columsn in customer_transactions_df, which will be persisted as final table.
    customer_transactions_df = final_df.select(col('transaction_id'),
                                               col('transaction_date'),
                                               col('customer_id'),
                                               col('country'),
                                               col('amount_in_eur'),
                                               col('valid_from'),
                                               col('valid_to'),
                                               col('last_transaction'))

    logger.info(f"Customer_Transactions dataset successfully created. Count = {customer_transactions_df.count()} ")
    customer_transactions_df.printSchema()
    customer_transactions_df.show(20)

    # Step 4 : Writing the final dataframe into Datawarehouse (GCP Bigquery/ Snowflake / AWS) :
    customer_transactions_df.persist()
    '''
    If we had to write to gcp , we could have use spark.write options : 
    Ex : 
    final_df.write.format("bigquery") \
        .option("project", project_id) \
        .option("dataset", dataset_id) \
        .option("table", table_id) \
        .save()
    '''

    # Step 5 : Creating final SQL views on top of this data :
    customer_transactions_df.createOrReplaceTempView("customer_transactions")

    # sql1 : Create a view which shows the total amount in EUR, number of transactions and number of unique users per month and country.
    transaction_view_df = analysis.transactions_view("customer_transactions",spark)
    logger.info("Monthly Transactions view successfully created :")
    transaction_view_df.show(20)

    # sql2 : Based on a date, show the evolution of the exchange rate for each currency (based on GBP) in percentage for the next 15 days
    exchange_rate_view_df = analysis.exchange_rate_view("Exchange_rates", spark, '2023-01-01')
    logger.info("Exchange rate view successfully created :")
    exchange_rate_view_df.show(20)

    # Capturing total ingestion time :
    end_time = datetime.datetime.now()
    logger.info(f"Pipeline ran Successfully. End time : {end_time}" )
    total_time = end_time - start_time
    logger.info(f"Pipeline run time : {total_time}" )

    spark.stop()

if __name__ == '__main__':
    main()