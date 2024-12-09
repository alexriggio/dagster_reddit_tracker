# Standard library imports
import os
import time
from datetime import datetime, timedelta

# Third-party imports
import pandas as pd
import duckdb
import prawcore
from dagster import AssetExecutionContext, EnvVar, asset, MetadataValue, MaterializeResult
from dagster_duckdb import DuckDBResource

# Local application-specific imports
from . import constants
from ..partitions import daily_partition
from ..resources.praw_resource import PRAWResource


@asset(
    partitions_def=daily_partition,
    group_name='data_ingestion',
)
def fetch_daily_reddit_posts(context: AssetExecutionContext, database: DuckDBResource, praw: PRAWResource) -> MaterializeResult:
    """
    Fetches Reddit posts from specified subreddits for a given date range
    and stores them in a database.
    """
    
    # Retrieve the partition date string
    partition_date_str = context.partition_key
    
    # Calculate the start and end date based on the partition date
    start_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
    end_date = start_date + timedelta(days=1)
    
    # Convert start and end dates to UNIX timestamps
    start_timestamp = int(time.mktime(start_date.timetuple()))
    end_timestamp = int(time.mktime(end_date.timetuple()))
    
    # Initialize the Reddit client
    reddit = praw.get_client()

    # Initialize variables
    posts_in_date_range = []
    retries = 5  # Number of retries
    delay = 3  # Delay between retries in seconds
    
    for subreddit_name in constants.SUBREDDITS:
        # Fetch posts
        subreddit = reddit.subreddit(subreddit_name)
        for attempt in range(retries):
            try:
                results = subreddit.new(limit=None)  # Fetch newest posts (up to ~1000)
                context.log.info(f"Fetched posts from subreddit: {subreddit_name}")
                
                # Filter posts by creation date
                for post in results:
                    if start_timestamp <= post.created_utc <= end_timestamp:
                        # Append relevant post details to the list
                        posts_in_date_range.append({
                            "post_id": post.id,
                            "subreddit": subreddit_name,
                            "title": post.title,
                            "permalink": post.permalink,
                            "url": post.url,
                            "created_utc": post.created_utc,
                            "created_local": datetime.fromtimestamp(post.created_utc).strftime("%Y-%m-%d %H:%M:%S"),
                            "score": post.score,
                            "n_comments": post.num_comments,
                        })
                break  # Exit retry loop if successful
            except prawcore.exceptions.ServerError as e:
                context.log.warning(f"Server error while fetching posts from {subreddit_name}: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    context.log.error(f"Retries exhausted for subreddit: {subreddit_name}")
                    raise
            except Exception as e:
                context.log.error(f"An unexpected error occurred: {e}")
                raise

    # Count the number of Reddit posts fetched from the subreddit - for dagster metadata
    n_reddit_posts = len(posts_in_date_range)
    context.log.info(f"Successfully fetched {n_reddit_posts} posts.")
    
    # Convert the list of posts into a Pandas DataFrame
    posts_df = pd.DataFrame(posts_in_date_range)
    
    # SQL query to create the `posts` table if it doesn't already exist 
    create_query = """
    CREATE TABLE IF NOT EXISTS posts (
        post_id TEXT PRIMARY KEY,
        humanoid TEXT DEFAULT NULL,
        subreddit TEXT,
        title TEXT,
        created_utc INTEGER,
        created_local TEXT,
        score INTEGER,
        n_comments INTEGER,
        partition_date VARCHAR(10)
    )
    """

    # SQL query to delete old data for the current partition
    delete_query = f"""
    DELETE FROM posts
    WHERE created_utc >= '{start_timestamp}'
    AND created_utc < '{end_timestamp}'
    """

    # SQL query to insert new data into the `posts` table
    insert_query = f"""
    INSERT INTO posts (
        post_id, 
        humanoid, 
        subreddit, 
        title, 
        created_utc, 
        created_local, 
        score, 
        n_comments, 
        partition_date 
    )
    SELECT
        post_id,
        NULL as humanoid,
        subreddit,
        title,
        created_utc,
        created_local,
        score,
        n_comments,
        '{partition_date_str}' AS partition_date
    FROM temp_posts_df
    """
    
    # Retry logic for database operations
    retries = 5
    for attempt in range(retries):
        try:
            with database.get_connection() as conn:
                # Register the DataFrame as a DuckDB temporary table
                conn.register("temp_posts_df", posts_df)
                # Execute SQL queries to create, delete, and insert data
                conn.execute(create_query)
                conn.execute(delete_query)
                conn.execute(insert_query)
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
    
    # Return a MaterializeResult to track metadata about this asset's execution
    return MaterializeResult(
        metadata={
            'Number of fetched posts': n_reddit_posts
        }
    )

@asset(
    deps=['fetch_daily_reddit_posts'],
    partitions_def=daily_partition,
    group_name='data_processing',
)
def classify_daily_reddit_posts(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    """
    Classifies daily Reddit posts based on their titles into what
    type of humanoid they are about (neo, figure, optimus) 
    and updates the database with classification labels.
    """
    
    # Retrieve the partition date string
    partition_date_str = context.partition_key
    
    # Calculate start and end timestamps for filtering posts
    start_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
    end_date = start_date + timedelta(days=1)
    
    # Convert start and end dates to UNIX timestamps
    start_timestamp = int(time.mktime(start_date.timetuple()))
    end_timestamp = int(time.mktime(end_date.timetuple()))
    
    # SQL query to select posts within the partition's date range
    select_query = f"""
    SELECT * 
    FROM posts
    WHERE created_utc >= '{start_timestamp}'
    AND created_utc < '{end_timestamp}';
    """
    
    # Retry logic for querying the database
    retries = 5
    for attempt in range(retries):
        try:
            with database.get_connection() as conn:
                # Query database and convert table to Pandas Dataframe
                posts_partition_df = conn.execute(select_query).fetch_df()
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
    
    # Function to classify a post based on its title
    def get_post_labels(title):
        labels = []
        robots = {
            "optimus": ["robot", "bot", "humanoid"],
            "figure": ["01", "02", "03", "humanoid", "robot", "bot"],
            "neo": ["1x", "humanoid", "robot", "bot"],
        }
        exclusion_patterns = ["to figure", "figure out", "figure it out"]
        neo_patterns = ["1x bot", "1x robot", "1x humanoid"]
        # Standardize title to lowercase
        title = title.lower()
        
        for robot, keywords in robots.items():
            # If `optimus` is in title or `tesla` plus one of the keywords 
            if robot == 'optimus':
                if 'optimus' in title or ('tesla' in title and any(k in title for k in keywords)):
                    labels.append(robot)
            # If `figure` is in title plus one of the keywords and not an exclusonary phrase 
            elif robot == 'figure':
                if 'figure' in title and any(k in title for k in keywords):
                    if not any(exclusion in title for exclusion in exclusion_patterns):
                        labels.append(robot)
            # If `neo` is in the title plus one of the keywords or `1x` directly followed by a robot related word
            else:
                if ('neo' in title and any(k in title for k in keywords)) or any(k in title for k in neo_patterns):
                    labels.append(robot)
        
        # If not classified neo, optimus, or figure than either a generic `humanoid` label or `none`
        if len(labels) == 0:
            if 'humanoid' in title:
                labels.append('other')
            else:
                labels.append('none')    

        # If title is assigned multiple labels than hyphenate (ie. optimus-neo) otherwise return single label
        return "-".join(labels) if len(labels) > 1 else labels[0]
    
    # Apply the classification logic to the `title` column
    posts_partition_df["humanoid"] = posts_partition_df["title"].apply(get_post_labels)

    
    # Calculate the number of classified posts - for dagster metadata
    n_classified_posts = len(posts_partition_df)
    context.log.info(f"Successfully classified {n_classified_posts} posts.")
    
    # Count the occurrences of each humanoid category - for dagster metadata
    category_counts = posts_partition_df["humanoid"].value_counts().to_dict()
    
    # SQL query to update the `humanoid` column in the database
    update_query = """
    UPDATE posts
    SET humanoid = (
        SELECT humanoid
        FROM temp_updates
        WHERE temp_updates.post_id = posts.post_id
    )
    WHERE post_id IN (SELECT post_id FROM temp_updates);
    """
    
    # Retry logic for the update operation
    retries = 5
    for attempt in range(retries):
        try:
            with database.get_connection() as conn:
                # Register the DataFrame as a temporary table
                conn.register("temp_updates", posts_partition_df)
                # Perform the batch update
                conn.execute(update_query)
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
    
    # Return a MaterializeResult to track metadata about this asset's execution
    return MaterializeResult(
    metadata={
        "Number of classified posts": MetadataValue.int(n_classified_posts),
        **{f"Count of {category}": MetadataValue.int(count) for category, count in category_counts.items()},
    }
)
    
@asset(
    deps=['classify_daily_reddit_posts'],
    partitions_def=daily_partition,
    group_name='data_processing',
)
def select_robot_posts(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    """
    Selects posts classified as robot-related and stores them in a separate table.
    """
    
    # Retrieve the partition date string and calculate the date range
    partition_str = context.partition_key
    start_date = datetime.strptime(partition_str, '%Y-%m-%d')
    end_date = start_date + timedelta(days=1)
    
    # SQL query to create a new table for robot-related posts
    create_table_query = """
    CREATE TABLE IF NOT EXISTS robot_posts AS
    SELECT * FROM posts WHERE 0=1;
    """
    
    # SQL query to delete existing data for the current partition
    delete_query = f"""
    DELETE FROM robot_posts
    WHERE created_local >= '{start_date}' AND created_local < '{end_date}';
    """

    # SQL query to insert robot-related posts into the `robot_posts` table
    insert_query = f"""
    INSERT INTO robot_posts
    SELECT *
    FROM posts
    WHERE humanoid IN ('optimus', 'figure', 'neo') 
    AND created_local >= '{start_date}' AND created_local < '{end_date}';
    """
    
    # Retry logic for database operations
    retries = 5
    for attempt in range(retries):
        try:
            with database.get_connection() as conn:
                 # Execute SQL queries to create, delete, and insert data 
                conn.execute(create_table_query)
                conn.execute(delete_query)
                result = conn.execute(insert_query)
                n_rows_inserted = result.rowcount  # Retrieve the number of rows inserted - for dagster metadata
                context.log.info(f"Query executed successfully. Rows inserted: {n_rows_inserted}")
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
    
    # Return a MaterializeResult to track metadata about this asset's execution
    return MaterializeResult(
    metadata={
        "Number of robot posts inserted": MetadataValue.int(n_rows_inserted),
    }
)