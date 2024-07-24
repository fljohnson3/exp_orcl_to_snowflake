from setuptools import find_packages, setup

setup(
    name="orcl_to_snowflake",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "dagster",
        # snowflake-connector-python[pandas] is included by dagster-snowflake-pandas but it does
        # not get included during pex dependency resolution, so we directly add this dependency
        "snowflake-connector-python[pandas]",
        "dagster-snowflake-pandas",
        "dagster-cloud",
        "boto3",
        "pandas",
        "matplotlib",
        "textblob",
        "tweepy",
        "wordcloud",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
