
from pyspark.sql.functions import row_number
from pyspark.sql.functions import round, col, expr, when, lag, lead
from pyspark.sql.window import Window
import os
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def dedup(df, partition_cols, order_by_cols):
    """
    Deduplicates a DataFrame based on partition columns and order by columns.

    Args:
        df (DataFrame): The input DataFrame to deduplicate.
        partition_cols (list): List of column names to partition the data.
        order_by_cols (list): List of column names to order the data within each partition.

    Returns:
        DataFrame: The deduplicated DataFrame.
    """
    window_spec = Window.partitionBy(partition_cols).orderBy(order_by_cols)
    deduplicated_df = df.withColumn("row_number", row_number().over(window_spec)).where("row_number = 1").drop(
        "row_number")


    return deduplicated_df


def convert_data_types(df, columns, data_types):
    """
    Convert the data types of columns in a DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        columns (list): List of column names to convert the data types.
        data_types (list): List of target data types for each column.

    Returns:
        DataFrame: The DataFrame with converted data types.
    """
    converted_df = df
    for col_name, data_type in zip(columns, data_types):
        converted_df = converted_df.withColumn(col_name, col(col_name).cast(data_type))
    return converted_df


def currency_conversion(exchange_rate_df, to_currency ):
    """
    This function converts the exchange from all currencies to one specific currency

    Args:
        exchange_rate_df (DataFrame): The exchange rate dataframe.
        to_currency (string): Currency to convert to.

    The function first changes all currencies to 'GBP' and then convert it to given currency

    Returns:
        DataFrame: The dataframe with conversion rates from all currencies to same currency.
    """
    return \
        exchange_rate_df.select(col('effective_date'),
                                col('to_currency').alias('from_currency'),
                                col('from_currency').alias('to_currency'),
                                (1/col('rate')).alias('rate'))\
                                .alias('d1')\
                                .join(exchange_rate_df.alias('d2'), col('d1.effective_date') == col('d2.effective_date') , 'left' )\
                                .where(col('d2.to_currency') == to_currency )\
                                .select(col('d1.effective_date'),
                                        col('d1.from_currency'),
                                        col('d2.to_currency'),
                                        round(col('d1.rate')*(col('d2.rate').cast('double')),3)
                                        .alias('rate'))


def load_data(final_df, output_path, log):
    """Load data from spark dataframe to CSV output location
    :param final_df: Datafrmae obtained after transforming the data
    :param output_path : Location for the output path where to write csv file
    :param log : log session available in start_spark
    :return: None
    """
    try:
        final_df.coalesce(1)\
                .write.option("header",True)\
                .option("delimiter","|")\
                .csv(os.path.join(ROOT_DIR, output_path))
        log.warn("Successfully loaded final dataframe to output location")

    except FileNotFoundError:
        log.error(f"{output_path} Not found")
        raise FileNotFoundError(f'{os.path.join(ROOT_DIR, output_path)} Not found')

