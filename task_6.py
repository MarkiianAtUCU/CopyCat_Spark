from utils import split_by_category
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col


def task_6(df, categories_map):
    window_video_id = Window.partitionBy('video_id').orderBy(col('ratio').desc())
    window_category = Window.partitionBy('category_id').orderBy(col('ratio').desc())

    task_6_res_df = df.where(df.views > 100_000).select(
        'category_id',
        'video_id',
        'title',
        'views',
        (col('likes') / col('dislikes')).alias('ratio')
    ).withColumn("rn", F.row_number().over(window_video_id)).filter(col('rn') == 1). \
        withColumn('rank', F.row_number().over(window_category)).filter(col('rank') <= 10)
    query_result = task_6_res_df.collect()

    json_result = {
        "categories": [
            {
                "category_id": category[0].category_id,
                "category_name": categories_map[category[0].category_id],
                "videos": [
                    {
                        "video_id": row.video_id,
                        "video_title": row.title,
                        "ratio_likes_dislikes": row.ratio,
                        "views": row.views
                    }
                    for row in category
                ]
            }
            for category in split_by_category(query_result, "category_id")
        ]
    }
    return json_result
