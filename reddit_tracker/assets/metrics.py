# Standard library imports
import time
from datetime import datetime, timedelta

# Third-party library imports
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import duckdb

# Local application/library imports
from dagster import asset, AssetExecutionContext, MetadataValue, MaterializeResult
from dagster_duckdb import DuckDBResource
from . import constants
from ..partitions import daily_partition


@asset(
    deps=['classify_daily_reddit_posts'],
    partitions_def=daily_partition,
    group_name='data_processing',
)
def aggregate_posts_by_category_and_week(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    """
    Aggregates Reddit posts by humanoid category on a weekly basis.

    This method collects weekly metrics (e.g., post counts, average scores, average comments)
    for each humanoid category and stores the results in the `weekly_post_metrics` table.
    """
    
    # Retrieve the partition date string and calculate the weekly date range starting on Monday (Monday = 0)
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, '%Y-%m-%d')
    start_date = partition_date - timedelta(days=partition_date.weekday())
    end_date = partition_date + timedelta(days=1)
    
    # Convert start and end dates to UNIX timestamps
    start_timestamp = int(time.mktime(start_date.timetuple()))
    end_timestamp = int(time.mktime(end_date.timetuple()))
    
    # Convert timestamp to string to perform comparison with partition_date str in delete query
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Convert to string to insert into the partition_date field of insert and select queries
    start_date_str = datetime.strftime(start_date, '%Y-%m-%d')

    # SQL query to create a sequence for generating unique IDs
    create_sequence_query = """  
    CREATE SEQUENCE IF NOT EXISTS seq_weekly_id START 1;
    """
    
    # SQL query to create the `weekly_post_metrics` table if it does not exist
    create_query = """
    CREATE TABLE IF NOT EXISTS weekly_post_metrics (
        id INTEGER PRIMARY KEY,
        humanoid TEXT,
        n_posts INTEGER,
        avg_score DOUBLE,
        avg_comments DOUBLE,
        partition_date VARCHAR(10)
    )
    """
    
    # SQL query to delete previous weekly data in case of re-execution    
    delete_query = f"""  
    DELETE FROM weekly_post_metrics
    WHERE partition_date >= '{start_date_str}'
    AND partition_date < '{end_date_str}';
    """
    
    # SQL query to insert aggregated metrics for the weekly partition
    insert_query = f"""
    INSERT INTO weekly_post_metrics (id, humanoid, n_posts, avg_score, avg_comments, partition_date)
    SELECT
        nextval('seq_weekly_id'),
        humanoid,
        COUNT(*) AS n_posts,
        ROUND(AVG(score), 2) AS avg_score,
        ROUND(AVG(n_comments), 2) AS avg_comments,
        '{start_date_str}' AS partition_date
    FROM posts
    WHERE created_utc >= '{start_timestamp}'
    AND created_utc < '{end_timestamp}'
    GROUP BY humanoid
    """
    
    # SQL query to select the aggregated metrics after being inserted
    select_query = f"""
    SELECT humanoid, n_posts, avg_score, avg_comments
    FROM weekly_post_metrics
    WHERE partition_date = '{start_date_str}'
    """
    
    # Retry logic for database operations
    retries = 5
    for attempt in range(retries):
        try:
            with database.get_connection() as conn:
                # Execute SQL queries to create a sequence 
                # in addition to create, delete, and insert data
                # and select that data to be used for dagster metadata
                conn.execute(create_sequence_query)
                conn.execute(create_query)
                conn.execute(delete_query)
                conn.execute(insert_query)
                aggregated_data = conn.execute(select_query).fetch_df()
                context.log.info("Query executed successfully.")
                break  # Exit loop if successful
        except duckdb.IOException as e:
            if attempt < retries - 1:
                context.log.warning(f"Retrying due to error: {e}")
                time.sleep(1)
            else:
                context.log.error("Retries exhausted. Raising error.")
                raise
        except Exception as e:
            context.log.error(f"An unexpected error occurred: {e}")
            raise
    
    # Return metadata
    return MaterializeResult(
        metadata={
            "Total post count": MetadataValue.int(int(aggregated_data["n_posts"].sum())),
            **{
                f"Post count of {row['humanoid']}": MetadataValue.int(int(row["n_posts"]))
                for _, row in aggregated_data.iterrows()
            },
            **{
                f"Avg comment count for {row['humanoid']}": MetadataValue.float(row["avg_comments"])
                for _, row in aggregated_data.iterrows()
            },
            **{
                f"Avg post score for {row['humanoid']}": MetadataValue.float(row["avg_score"])
                for _, row in aggregated_data.iterrows()
            },
        }
    )
        
@asset(
    deps=['aggregate_posts_by_category_and_week'],
    partitions_def=daily_partition,
    group_name='data_analysis',
)
def plot_weekly_post_counts(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """
    Generates a line plot for the weekly post counts by humanoid category.

    This method fetches weekly aggregated metrics from the `weekly_post_metrics` table,
    processes the data, and generates a time-series line plot for each humanoid category.

    """
    
    # SQL query to select all data from the `weekly_post_metrics` table
    select_query = """SELECT * FROM weekly_post_metrics"""
    
    # Retres to handle database query failures
    retries = 5
    for attempt in range(retries):
        try:
            with database.get_connection() as conn:
                # Query database and convert table to Pandas Dataframe
                df = conn.execute(select_query).fetch_df()
                context.log.info("Query executed successfully.")
                break  # Exit loop if successful
        except duckdb.IOException as e:
            if attempt < retries - 1:
                context.log.warning(f"Retrying due to error: {e}")
                time.sleep(1)
            else:
                context.log.error("Retries exhausted. Raising error.")
                raise
        except Exception as e:
            context.log.error(f"An unexpected error occurred: {e}")
            raise

    # Define the relevant humanoid categories
    categories = ["optimus", "figure", "neo"]
    
    # Expand hyphenated categories into multiple rows to attribute a count to each relevant category
    expanded_rows = []
    for _, row in df.iterrows():
        humanoids = row["humanoid"].split("-")  # Split hyphenated categories
        for humanoid in humanoids:
            if humanoid not in ['none', 'other']:  # Ignore 'none' and 'other'
                new_row = row.copy()
                new_row["humanoid"] = humanoid
                expanded_rows.append(new_row)

    # Convert the expanded rows into a new DataFrame
    expanded_df = pd.DataFrame(expanded_rows)
    
    # Ensure partition_date is in datetime format and sort by date
    expanded_df["partition_date"] = pd.to_datetime(expanded_df["partition_date"])
    expanded_df = expanded_df.sort_values(by="partition_date")

    # Group by week and humanoid, then sum post count
    grouped = (
        expanded_df.groupby(["partition_date", "humanoid"])["n_posts"]
        .sum()
        .unstack(fill_value=0)  # fills missing combinations of partition_date and humanoid to avoid NaN values in the pivoted table
    )

    # Reindex to ensure all categories are present
    grouped = grouped.reindex(columns=categories, fill_value=0)
    
    # Reformat the date in the index to how it will display on x-axis
    grouped.index = grouped.index.strftime("%b-%d-%y") 

    # Plotting
    grouped.plot(kind="line", figsize=(10, 6), linewidth=2.5, marker="o", alpha=0.8)

    # Customize y-axis ticks based on the maximum value in the data
    max_y = grouped.values.sum(axis=1).max()
    plt.yticks(range(0, int(max_y) + 2, 1))  # Increment by 1

    # Rotate x-axis ticks for better visibility
    plt.xticks(rotation=90)

    # Add labels and title
    plt.xlabel("Partition Date")
    plt.ylabel("Number of Posts")
    plt.title("Weekly Post Metrics by Humanoid Category")
    plt.legend(title="Humanoid")

    # Adjust layout and save the plot to a file
    plt.tight_layout()
    plt.savefig(constants.WEEKLY_PLOT_FILE_PATH)