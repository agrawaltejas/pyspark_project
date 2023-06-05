# PySpark Project

This document is designed to use best practices of Spark and Python to process the csv file from input , transform it, and load it to final location.

## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- utilities/
     |-- analysis.py
     |-- helper.py
 |   |-- logging.py
 |   |-- log4j.properties
 |-- src/
 |   |-- main
         |-- main.py
     |-- tests 
         |-- test_main.py
 |   data
 |   output
```

The main Python module containing the ETL job (which will be sent to the Spark cluster), is `src/main/main.py`. Unit test modules are kept in the `src/main/tests` folder .
`Utilities` folder has `helper` function and `logger` properties for the file. We are sending logger config externally while running spark-submit.
`Data` folder has 3 input .csv files. `Output` will store the final dataframe in .csv.
`helper/analysis.py` is having 2 spark-sql queries on top of final dataset to take aggregated insights.

We can include `config` folder for including any configs like kafka offset. 
Also, `dependencies` folder can be added to note any external dpependencies, package it and then send to the job.
But we don't need these in our case here.

## Steps used for ETL Job : 

- Step 1 : Reading/ingesting data from the source specified. In our case it is csv files in 'data' folder.
- Step 2 : Cleanse and validate the data. We have used `dedup` and `data_type_conversion` functions from `helper` class.
- Step 3 : Store/persist the data in data lake. Here, we are persisting it in spark memory. 
- Step 4 : Join the 3 tables, and create a final transformed dataset according to data model. Here we are doing majorly 2 transformations : i) Joining country to transactions and changing all currency to `EUR`, which is mostly used currency in the data. ii) Adding `valid_from` and `valid_to` to make note of history and last transaction for a customer. 
- Step 6 : 2 analysis `SQLs` on top of final dataset to create aggregate insights. Stored in `utilities/analysis.py` location. We are using `Spark-Sql` here. 
- Step 7 : Stop the spark session and log the run times, final count etc as `log metrics`. 
- Note : unit tests are placed in `src/tests/test_main.py`. Use command : `pipenv run python -m unittest src/tests/test_main.py` to run the tests. 

## Running the ETL job [Local Mode - Pipenv]

- We need to have first installed Python globally in local. I am using Python 3.10 version.;
- As I am using `Pipenv`, we need to install it and load pyspark, py4j dependencies ;
- Install Pipenv 
    ```bash 
   pip install pipenv
   pip install --upgrade pipenv
    ``` 
- If terminal fails to identify Pipenv , we may need to add Scripts path to PATH variable :
    ```bash 
   python -m site --user-site
   #example result : C:\Users\tejas\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.10_qbz5n2kfra8p0\LocalCache\local-packages\Python310\site-packages
   #Add this Path to PATH variable , with `site-packages` replaced with `Scripts` : 
   setx PATH "%PATH%;C:\Users\tejas\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.10_qbz5n2kfra8p0\LocalCache\local-packages\Python310\Scripts"
    ```  
- Install pyspark
    ```bash 
   pip install pyspark
   # This will install development dependencies lile py4j , pyspark etc
    ```  
- Now , we are good to go with running our Pyspark application using Pipenv : 
     ```bash
    $ spark-submit --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=utilities/log4j.properties src/main/main.py
    OR 
    $ pipenv run python3 src/main/main.py
    ```  
- We will use following command to run the unitest cases :
     ```bash 
     pipenv run python -m unittest src/tests/test_main.py
    ```  

## Running the ETL job [Cluster Mode]

Assuming that the `$SPARK_HOME` environment variable points to your local Spark installation folder, then the ETL job can be run from the project's root directory using the following command from the terminal,

```bash
$SPARK_HOME/bin/spark-submit \
--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=utilities/log4j.properties \
--master local[*] \
src/main/main.py
```

Briefly, the options supplied serve the following purposes:

- `--master local[*]` - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation (either in single-executor mode locally, or something larger in the cloud) and want to send the job there, then modify this with the appropriate Spark IP - e.g. `spark://the-clusters-ip-address:7077`;
- `--conf spark.driver.extraJavaOptions'` - Setting default log properties to custom log4j.properties location.
- `src/main/main.py` - the Python module file containing the ETL job to execute.


### Automatic Loading of Environment Variables

Pipenv will automatically pick-up and load any environment variables declared in the `.env` file, located in the package's root directory. For example, adding,

```bash
SPARK_HOME=applications/spark-2.3.1/bin
HADOOP_HOME=application/hadoop-2.7.1/bin
DEBUG=1
```

## Performance Tuning

We can use many practices to optimize the code pipeline and increase the runtime if data gets huge:

- Use of Parquet file format for storing final files, as it's best suitable for storing row columnar data;
- Use of colaesce() to process the data with partitions and without shuffling.
- Use of correct executors,cores and memory in spark can do wonders when we need to process high amount of data. With strong knowledge of architecture and trial method, we can find the best settings as per our requirements;
- We should persist the data if it is going to be used multiple times.
- We can avoid shuffling operations such as groupByKey(),reduceByKey() , and should replace joins with window functions wherever possible;
- We should use less actions in the pipeline such as count() or show() as it computes the whole DAG again and again at every action , which can reduce running time & lazy evaluation will not take effect to it's fullest.
- We should test our pipelines with peak testing data , to see if our pipelines are handling the huge data well.

## Production Deployment 

We may use production environment platforms such as oozie, airflow etc to deploy our pipelines. In our case, we can create a JAR with dependencies , config files and main entry class, and should use this JAR as an item in the workflow. We should have checks in place to see the workflow goes ahead only if this item runs successfully.

We can trigger the workflow intraday/daily/weekly based on our requirement. 

