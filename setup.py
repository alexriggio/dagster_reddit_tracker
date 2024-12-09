from setuptools import find_packages, setup

setup(
    name="reddit_tracker",
    packages=find_packages(exclude=["reddit_tracker_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-openai",
        "pandas",
        "praw",
        "matplotlib",
        "reportlab"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
