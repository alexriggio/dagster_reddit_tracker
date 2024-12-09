from dagster import DailyPartitionsDefinition, WeeklyPartitionsDefinition, MonthlyPartitionsDefinition

from ..assets import constants


start_date = constants.START_DATE

daily_partition = DailyPartitionsDefinition(
    start_date=start_date,
    end_date=None,
    fmt='%Y-%m-%d',
)

# weekly_partition = WeeklyPartitionsDefinition(
#     start_date=start_date,
#     end_date=None,
#     fmt='%Y-%m-%d',
# )
