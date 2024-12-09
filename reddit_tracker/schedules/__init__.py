from dagster import ScheduleDefinition
from ..jobs import daily_update_job


daily_update_schedule = ScheduleDefinition(
    job=daily_update_job,
    cron_schedule="0 0 * * *", # every day at midnight
)

# weekly_update_schedule = ScheduleDefinition(
#     job=weekly_update_job,
#     cron_schedule="0 0 * * 5", # every Friday at midnight
# )
