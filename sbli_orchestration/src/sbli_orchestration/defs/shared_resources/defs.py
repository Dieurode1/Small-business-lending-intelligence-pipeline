"""
Register shared resources so load_from_defs_folder auto-discovers them.

The dict key "snowflake" must match the parameter name in the asset
functions (e.g. fred_raw_load(context, snowflake)), which is how Dagster
injects the resource automatically.
"""
import dagster as dg

from sbli_orchestration.defs.extractors.resources import snowflake_resource


@dg.definitions
def defs():
    return dg.Definitions(
        resources={
            "snowflake": snowflake_resource,
        }
    )
