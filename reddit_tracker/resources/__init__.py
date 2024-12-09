from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_openai import OpenAIResource

from .praw_resource import PRAWResource


database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE")
)

openai_resource = OpenAIResource(
    api_key=EnvVar("OPENAI_API_KEY"),
)

praw_resource = PRAWResource(
    client_id=EnvVar("REDDIT_CLIENT_ID"),
    client_secret=EnvVar("REDDIT_CLIENT_SECRET"),
    user_agent=EnvVar("REDDIT_USER_AGENT"),
)