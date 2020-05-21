import pyspark.sql.functions as F


def task_1(df):
    # Little hack due to different column names "description" and "description\r" in different files
    description_column_name = df.columns[-1]
    task1_res_df = df.groupBy("video_id").count().orderBy('count', ascending=False).limit(10). \
        join(df, 'video_id', 'inner').groupBy('video_id').agg(
        F.first("title").alias("title"),
        F.first(description_column_name).alias("description"),
        F.collect_list('trending_date').alias("trending_date_list"),
        F.collect_list('views').alias('view_list'),
        F.collect_list('likes').alias('like_list'),
        F.collect_list('dislikes').alias("dislike_list")
    )
    query_result = task1_res_df.collect()

    json_result = {
        "videos":
            [
                {
                    "id": row.video_id,
                    "title": row.title,
                    "description": row.description,
                    "latest_views": row.view_list[-1],
                    "latest_likes": row.like_list[-1],
                    "latest_dislikes": row.dislike_list[-1],
                    "trending_days": [
                        {
                            "date": row.trending_date_list[i],
                            "views": row.view_list[i],
                            "likes": row.like_list[i],
                            "dislikes": row.dislike_list[i],
                        } for i in range(len(row.trending_date_list))
                    ]
                }
                for row in query_result
            ]
    }
    return json_result
