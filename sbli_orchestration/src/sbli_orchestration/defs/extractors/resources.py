"""
Snowflake resource for the RAW-load assets.

Mirrors the dbt project's connection (~/.dbt/profiles.yml): key-pair (JWT)
auth via the unencrypted .p8 private key, same account/user/role/warehouse.
Because the key is unencrypted, no private_key_password is supplied.

The RAW loads run COPY INTO statements *inside Snowflake*; Snowflake reaches
S3 via the SBLI_S3_INTEGRATION storage integration, so Dagster itself needs
no AWS credentials for this step — only a Snowflake connection.
"""
from dagster_snowflake import SnowflakeResource

snowflake_resource = SnowflakeResource(
    account="evc47002.us-east-1",
    user="DIEURODE1994",
    role="SBLI_ROLE",
    warehouse="SBLI_WH",
    database="SBLI",
    private_key_path="/Users/ghola/.dbt/keys/sbli_dbt_key.p8",
    authenticator="snowflake_jwt",
)
