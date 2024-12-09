from dagster import AssetSelection, define_asset_job
from ..partitions import daily_partition


daily_update_job = define_asset_job(
    name='daily_update_job',
    partitions_def=daily_partition,
    selection=[
        'fetch_daily_reddit_posts',
        'classify_daily_reddit_posts',
        'select_robot_posts',
        'summarize_robot_posts',
        'aggregate_posts_by_category_and_week', 
        'plot_weekly_post_counts',
    ],
)

# weekly_update_job = define_asset_job(
#     name='weekly_update_job',
#     partitions_def=weekly_partition,
#     selection=[

#     ],
# )

generate_pdf_report_job = define_asset_job(
    name='generate_pdf_report_job',
    selection='generate_pdf_reports',
)