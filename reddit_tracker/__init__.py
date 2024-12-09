from dagster import Definitions, load_assets_from_modules

from .assets import posts, metrics, summaries, reports
from .resources import database_resource, openai_resource, praw_resource
from .jobs import daily_update_job, generate_pdf_report_job
from .schedules import daily_update_schedule
from .sensors import generate_pdf_reports_sensor


# Load assets from their respective modules
post_assets = load_assets_from_modules([posts])
metric_assets = load_assets_from_modules([metrics])
summaries_assets = load_assets_from_modules([summaries])
reports_assets = load_assets_from_modules([reports])


# Collect all jobs, schedules, and sensors
all_jobs = [daily_update_job, generate_pdf_report_job]
all_schedules = [daily_update_schedule]
all_sensors = [generate_pdf_reports_sensor]


# Define the Dagster definitions object
defs = Definitions(
    assets=[*post_assets, *metric_assets, *summaries_assets, *reports_assets],
    resources={
        'database': database_resource,
        'openai': openai_resource,
        'praw': praw_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
)

