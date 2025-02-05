from setuptools import find_packages, setup

setup(
    name="proply",
    packages=find_packages(exclude=["proply-tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "requests",
        "boto3",
        "psycopg2",
        "paramiko",
        "beautifulsoup4",
        "stream-unzip",
        "httpx",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
