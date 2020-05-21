import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col


def task_4(df):
    window_duplicate_remove = Window.partitionBy('video_id').orderBy(col('views').desc())

    task_4_res_df = df.select('channel_title', 'video_id', 'views', 'trending_date',
                              F.row_number().over(window_duplicate_remove).alias('rn')). \
        filter(col('rn') == 1).groupBy("channel_title").agg(
        F.first('trending_date').alias("start_date"),
        F.last('trending_date').alias("end_date"),
        F.sum('views').alias("total_views"),
        F.collect_list('video_id').alias("video_id_list"),
        F.collect_list('views').alias("views_list")
    ).orderBy(col('total_views'), ascending=False).limit(20)
    query_result = task_4_res_df.collect()
    json_result = {
        "channels": [
            {
                "channel_name": row.channel_title,
                "start_date": row.start_date,
                "end_date": row.end_date,
                "total_views": row.total_views,
                "videos_views": [
                    {
                        "video_id": row.video_id_list[i],
                        "viewes": row.views_list[i]
                    }
                    for i in range(len(row.video_id_list))
                ]
            }
            for row in query_result]
    }
    return json_result
