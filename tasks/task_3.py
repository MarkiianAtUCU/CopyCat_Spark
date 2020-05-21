from utils.utils import date_by_month_from, date_by_month_to, split_by_category
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col


def task_3(df_proper_date):
    window_group_MoY = Window.partitionBy("MoY").orderBy(col('video_num').desc())
    window_duplicate_remove = Window.partitionBy('video_id').orderBy(col('views').desc())
    task_3_res_df = df_proper_date.select(
        (F.month(df_proper_date.dateframe) + F.year(df_proper_date.dateframe) * 12).alias("MoY"),
        "video_id",
        'tags',
        F.row_number().over(window_duplicate_remove).alias('rank')
    ).filter(col('rank') == 1).withColumn("tag_exploded", F.explode(F.split(col("tags"), '\\|'))).groupBy("MoY",
                                                                                                          "tag_exploded").agg(
        F.count('video_id').alias('video_num'),
        F.collect_list('video_id').alias('video_id_list')
    ).withColumn('rank', F.row_number().over(window_group_MoY)).filter(col('rank') <= 10)
    query_result = task_3_res_df.collect()
    json_result = {
        "months":
            [
                {
                    "start_date": date_by_month_from(int(month[0].MoY / 12), month[0].MoY % 12 + 1),
                    "end_date": date_by_month_to(int(month[0].MoY / 12), month[0].MoY % 12 + 1),
                    "tags": [
                        {
                            "tag": row.tag_exploded,
                            "number_of_videos": row.video_num,
                            "video_ids": row.video_id_list
                        }
                        for row in month
                    ]
                }
                for month in split_by_category(query_result, "MoY")
            ]
    }
    return json_result
