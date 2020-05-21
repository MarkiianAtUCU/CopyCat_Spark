from collections import Counter
import pyspark.sql.functions as F


def task_5(df):
    task_5_res_df = df.groupBy("channel_title").count().orderBy('count', ascending=False). \
        limit(10).join(df, 'channel_title', 'left').groupBy('channel_title').agg(
        F.first('count').alias("total_trending_days"),
        F.collect_list('video_id').alias("video_id_list"),
        F.collect_list('title').alias("video_title_list")
    )
    query_result = task_5_res_df.collect()

    json_result = {
        "channels": [
            {
                "channel_name": row.channel_title,
                "total_trending_days": row.total_trending_days,
                "videos_days": [
                    {
                        "video_id": video_id,
                        "video_title": row.video_title_list[row.video_id_list.index(video_id)],
                        "trending_days": count
                    }

                    for video_id, count in Counter(row.video_id_list).items()
                ]
            }
            for row in query_result
        ]
    }
    return json_result
