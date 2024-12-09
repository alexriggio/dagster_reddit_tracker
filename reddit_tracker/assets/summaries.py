# Standard library imports
import os
import time
import json
from datetime import datetime, timedelta

# Third-party imports
import prawcore
import pandas as pd
import duckdb

# Dagster imports
from dagster import asset, AssetExecutionContext, EnvVar
from dagster_duckdb import DuckDBResource
from dagster_openai import OpenAIResource

# Local application imports
from . import constants
from ..resources.praw_resource import PRAWResource
from ..partitions import daily_partition


@asset(
    deps=['select_robot_posts'],
    partitions_def=daily_partition,
    group_name='data_analysis',
)
def summarize_robot_posts(context: AssetExecutionContext, database: DuckDBResource, openai: OpenAIResource, praw: PRAWResource) -> None:
    """
    Summarizes Reddit posts about humanoid robots for a given day and extracts relevant themes.
    """
    
    # Extract partition key (date string) and convert it to start and end datetime
    partition_date_str = context.partition_key
    start_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
    end_date = start_date + timedelta(days=1)
    
    # Convert datetime to Unix timestamp for querying
    start_timestamp = int(time.mktime(start_date.timetuple()))
    end_timestamp = int(time.mktime(end_date.timetuple()))
    
    # Query to fetch robot posts for the partitioned day
    select_query = f"""
        SELECT * 
        FROM robot_posts
        WHERE created_utc >= '{start_timestamp}'
        AND created_utc < '{end_timestamp}'
        AND humanoid IN ('optimus', 'figure', 'neo');
    """
    
    retries = 5
    for attempt in range(retries):
        try:
            with database.get_connection() as conn:
                partition_df = conn.execute(select_query).fetch_df()
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
        
    
    with openai.get_client(context) as client:
        # Define a prompt for OpenAI to summarize Reddit posts
        summarize_reddit_post_prompt = '''
            You are a marketer and product designer for a humanoid robotics company interested in the public's 
            perception of humanoid robots so that you may design your humanoid robot accordingly to meet
            the needs of consumers.

            Please provide a brief summary of the comments found within the reddit post,
            and also list out major themes pertinent to people's perceptions of the humanoid 
            robot that would be relevant to a marketer and product designer 
            (ie. concerned about safety, the robot is too big, likes how the robot walks etc.).    

            Return only a JSON object in the following format:

            {
                "summary": "summary text here",
                "themes": ["theme 1", "theme 2", ..., "theme n"]
            }
        '''

        def summarize_reddit_post(comments_str):
            """
            Summarizes Reddit post comments and extracts themes using OpenAI GPT.

            Args:
                comments_str (str): Flattened and concatenated Reddit comments.

            Returns:
                dict: JSON object containing "summary" and "themes".
            """
            
            # Make an API call to OpenAI to classify sentiment
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": summarize_reddit_post_prompt
                    },
                    {
                        "role": "user",
                        "content": comments_str
                    }
                ],
                temperature=0,  # Set low temperature to make output more deterministic
                response_format={
                    "type": "json_object"  # This is to enable JSON mode, making sure responses are valid json objects
                },
            )
            
            return response.choices[0].message.content
        
        
        # Initialize PRAW client for Reddit API
        reddit = praw.get_client()
        
        # Initialize variables
        retries = 5  # Number of retries
        delay = 1  # Delay between retries in seconds
        
        # Check if there are posts to process
        if len(partition_df) > 0:
            all_summaries = []    
            for _, row in partition_df.iterrows():
                for attempt in range(retries):
                    try:
                        # Fetch the post by ID
                        post = reddit.submission(id=row.post_id)
                        context.log.info(f"Fetched post: {row.post_id}")
                        break
                    except prawcore.exceptions.ServerError as e:
                        context.log.warning(f"Server error while fetching post: {row.post_id}: {e}")
                        if attempt < retries - 1:
                            time.sleep(delay)
                        else:
                            context.log.error(f"Retries exhausted for post: {row.post_id}")
                            raise
                    except Exception as e:
                        context.log.error(f"An unexpected error occurred: {e}")
                        raise
                
                 # Expand 'MoreComments' and fetch all comments
                post.comments.replace_more(limit=None)

                # Flatten the comments into a string
                flattened_comments = [
                    f"- {comment.body} (Score: {comment.score}, Author: {str(comment.author) if comment.author else '[deleted]'})"
                    for comment in post.comments.list()
                ]
                flattened_comments_str = "\n".join(flattened_comments)
                
                # Summarize comments and extract themes
                summary_json_str = summarize_reddit_post(flattened_comments_str)
                summary_dict = json.loads(summary_json_str)
                
                # Add metadata to the summary
                summary_with_metadata = {
                    "post_id": post.id,
                    "n_comments": post.num_comments,
                    "post_permalink": post.permalink,
                    "humanoid": row.humanoid,
                    "title": post.title,
                    **summary_dict,
                }
                all_summaries.append(summary_with_metadata)

            # Save the data to a JSON file
            output_file = constants.POST_SUMMARIES_TEMPLATE_FILE_PATH.format(partition_date_str)
            with open(output_file, "w") as f:
                json.dump(all_summaries, f, indent=4)  # indentation of 4 spaces for each nested level

            context.log.info(f"Saved {len(all_summaries)} posts to {output_file}")