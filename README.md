# reddit_tracker

## Overview
Reddit Tracker is a Dagster pipeline for monitoring subreddit discussions about humanoid robots. Designed as a foundational project to learn Dagster fundamentals, it runs automatically on a daily schedule, ingesting new posts, classifying them by specific humanoid robot models in the market, summarizing discussions, identifying major product themes, and generating weekly PDF reports for stakeholders. 

This proof of concept demonstrates how analytics can offer valuable insights for marketing and product teams, with potential for future expansion.

## Features
- Automatically fetches posts from specified subreddits on a daily schedule.
- Classifies posts by robot models such as "Optimus" and "Neo."
- Summarizes Reddit comments to provide quick insights into discussions.
- Identifies major product-focused themes, including user concerns and preferences, to inform product and marketing strategies.
- Generates comprehensive weekly PDF reports for stakeholders, compiling all the above information.

## Technologies Used
- **Dagster**: For orchestrating the pipeline.
- **DuckDB**: As the lightweight database for storing data.
- **PRAW**: Python Reddit API Wrapper for fetching Reddit posts and comments.
- **OpenAI**: For comment summarization and theme extraction.
- **Pandas, Matplotlib**: For data processing and visualization.
- **Reportlab**: For generating PDF reports.

## Getting Started

Follow these steps to set up and run Reddit Tracker:

### Step 1: Clone the Repository
```bash
git clone https://github.com/your-username/reddit_tracker.git
cd reddit_tracker
```

### Step 2: Set Up the Environment and Install Dependencies
Next, set up the default environment variables and install the project Python dependencies by running:

```bash
cd reddit_tracker
cp .env.example .env
pip install -e ".[dev]"
```
You will need provide your own PRAW and OPENAI API key in the .env file.

```
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USER_AGENT=your_reddit_user_agent
OPENAI_API_KEY=your_openai_api_key
```
To verify that the installation was successful and that you can run Dagster locally, run:

```bash
dagster dev
```

Navigate to localhost:3000, where you should see the Dagster UI.

### Step 3: Set Up the DuckDB Database
Create and initialize the DuckDB database in the `data/staging` directory by running the following commands:

```bash
mkdir -p data/staging  # Create the directory if it doesn't exist
duckdb data/staging/data.duckdb
```
