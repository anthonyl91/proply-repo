from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Definitions,
    EnvVar,
    load_assets_from_package_module,
)
from dagster._utils import file_relative_path
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from proply import assets

from .common.resources.postgres_resource import PostgresResource
from .common.resources.s3_resource import S3Resource
from .common.resources.s3_to_postgres_io_manager import S3ToPostgresIOManager
from .common.resources.ssh_resource import SSHResource

DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_project")
DBT_PROFILES_DIR = file_relative_path(__file__, "../dbt_project/config")

raw_data_assets = load_assets_from_package_module(assets)

dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "dbt_project").resolve(),
)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def proply_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


defs = Definitions(
    assets=[*raw_data_assets, proply_dbt_assets],
    resources={
        "s3_to_postgres_io_manager": S3ToPostgresIOManager(
            postgres_resource=PostgresResource(
                host=EnvVar("PG_HOST").get_value(),
                port=EnvVar("PG_PORT").get_value(),
                database=EnvVar("PG_DB").get_value(),
                username=EnvVar("PG_USER").get_value(),
                password=EnvVar("PG_PASSWORD").get_value(),
            ),
            postgres_ssh_resource=SSHResource(
                host=EnvVar("PG_HOST").get_value(),
                username=EnvVar("SSH_USER").get_value(),
                password=EnvVar("SSH_PASSWORD").get_value(),
            ),
        ),
        "postgres_resource": PostgresResource(
            host=EnvVar("PG_HOST").get_value(),
            port=EnvVar("PG_PORT").get_value(),
            database=EnvVar("PG_DB").get_value(),
            username=EnvVar("PG_USER").get_value(),
            password=EnvVar("PG_PASSWORD").get_value(),
        ),
        "postgres_ssh_resource": SSHResource(
            host=EnvVar("PG_HOST"),
            username=EnvVar("SSH_USER").get_value(),
            password=EnvVar("SSH_PASSWORD").get_value(),
        ),
        "dbt": DbtCliResource(project_dir=dbt_project),
        "s3": S3Resource(
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID").get_value(),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
            endpoint_url=EnvVar("AWS_ENDPOINT").get_value(),
        ),
    },
)
