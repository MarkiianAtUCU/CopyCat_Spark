from utils.utils import date_by_week_n_from, date_by_week_n_to
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col


def task_2(df_proper_date, categories_map):
    window_group_WoY = Window.partitionBy("WoY").orderBy(col('total_views').desc())
    task_2_res_df = df_proper_date.withColumn('WoY', (
                F.weekofyear(df_proper_date.dateframe) + F.year(df_proper_date.dateframe) * 53)). \
        groupBy('WoY', "category_id", "video_id").agg(
        F.count('video_id').alias("count"),
        F.first('views').alias("start_views"),
        F.last('views').alias("end_views"),
    ).filter(col('count') > 1). \
        withColumn("diff", (col("end_views") - col("start_views"))). \
        groupBy("WoY", "category_id").agg(
        F.sum('diff').alias("total_views"),
        F.collect_list('video_id').alias("video_id_list"),
    ).withColumn("rank", F.row_number().over(window_group_WoY)).filter(col("rank") == 1)
    query_result = task_2_res_df.collect()
    json_result = {
        "weeks":
            [
                {
                    "start_date": date_by_week_n_from(int(row.WoY / 53), row.WoY % 53),
                    "end_date": date_by_week_n_to(int(row.WoY / 53), row.WoY % 53),
                    "category_id": row.category_id,
                    "category_name": categories_map[row.category_id],
                    "number_of_videos": len(row.video_id_list),
                    "total_views": row.total_views,
                    "video_ids": row.video_id_list
                }
                for row in query_result
            ]
    }
    return json_result
