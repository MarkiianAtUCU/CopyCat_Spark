import findspark
import time

findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from task_1 import task_1
from task_2 import task_2
from task_3 import task_3
from task_4 import task_4
from task_5 import task_5
from task_6 import task_6

spark = SparkSession.builder.getOrCreate()


def process_file(zone, csv_file_name, categories_map):
    print("    DATA LOADING Started")
    start_time = time.time()

    df = spark.read.csv(csv_file_name, header=True, multiLine=True)
    print(f"      Data loaded in: {time.time() - start_time:.2f}s")
    df_proper_date = df.withColumn("dateframe", F.to_date(df.trending_date, 'yy.dd.MM'))

    print("    TASK 1 Started")
    task_start_time = time.time()
    task_1(df)
    print(f"      Task 1 finished in: {time.time() - task_start_time:.2f}s")

    print("    TASK 2 Started")
    task_start_time = time.time()
    task_2(df_proper_date, categories_map)
    print(f"      Task 2 finished in: {time.time() - task_start_time:.2f}s")

    print("    TASK 3 Started")
    task_start_time = time.time()
    task_3(df_proper_date)
    print(f"      Task 3 finished in: {time.time() - task_start_time:.2f}s")

    print("    TASK 4 Started")
    task_start_time = time.time()
    task_4(df)
    print(f"      Task 4 finished in: {time.time() - task_start_time:.2f}s")

    print("    TASK 5 Started")
    task_start_time = time.time()
    task_5(df)
    print(f"      Task 5 finished in: {time.time() - task_start_time:.2f}s")

    print("    TASK 6 Started")
    task_start_time = time.time()
    task_6(df, categories_map)
    print(f"      Task 6 finished in: {time.time() - task_start_time:.2f}s")
    print(f"    {zone} zone overall time: {time.time() - start_time:.2f}s")
