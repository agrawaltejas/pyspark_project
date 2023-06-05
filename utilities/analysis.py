import os
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))

# Create a view which shows the total amount in EUR, number of transactions and number of unique users per month and country.
def transactions_view(final_dataframe, spark ) :

    return spark.sql("SELECT " \
              "DATE_FORMAT(transaction_date, 'yyyy-MM') AS month, " \
              "country, " \
              "COUNT(DISTINCT customer_id) AS unique_users, " \
              "COUNT(transaction_id) AS transaction_count, " \
              "SUM(amount_in_eur) AS total_amount_eur " \
              "FROM " \
              f"{final_dataframe} " \
              "GROUP BY " \
              "month, " \
              "country")


#Based on a date, show the evolution of the exchange rate for each currency (based on GBP) in percentage for the next 15 days
def exchange_rate_view(exchange_rate_df, spark, start_date) :

    return spark.sql(f"select " 
              f"from_currency, " 
              f"to_currency," 
              f"effective_date,"
              f"((LEAD(rate) OVER (PARTITION BY to_currency ORDER BY effective_date) - rate) / rate) * 100 AS rate_change_percentage " 
              f"from " 
              f"{exchange_rate_df} " 
              f"where " 
              f"from_currency = 'GBP' " 
              f"and DATE(effective_date) BETWEEN to_date({start_date}) AND date_add(to_date({start_date}), 15) " 
              f"ORDER BY effective_date ")