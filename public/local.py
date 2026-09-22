# DATA QUALITY RUNNER - FINAL CELL LAYOUT
# ============================================================
# Paste each section into the notebook cell indicated below.
#
# Architecture:
#   CONFIG
#      -> PLATFORM-SPECIFIC SOURCE ADAPTER
#      -> COMMON DATAFRAME
#      -> S3 DEFAULT + CUSTOM RULES
#      -> GX EXPECTATIONS
#      -> VALIDATION
#      -> S3 RESULTS
#
# IMPORTANT:
# - Athena/sqlutils exists ONLY in the SMUS source adapter.
# - Databricks uses Spark in its source adapter.
# - Default and custom rules stay separate in S3.
# - GX objects are runtime objects, not the persistent rule source.
# ============================================================


# ============================================================
# CELL 1 - CONFIGURATION
# ============================================================

DATASOURCE = "cdh-dpaas9768datasource-452482"
DATASET = "dpaas9768_ds_tbm"
STAGE = "raw"

# Use "smus_athena" in SMUS.
# Use "databricks" in Databricks.
SOURCE_TYPE = "smus_athena"

# SMUS/Athena only
CATALOG = "cdh_dpaas9768prj_72283"
TABLE = "dpaas9768_ds_tbm_raw"

DEFAULT_RULES_PATH = (
    f"s3://{DATASOURCE}/{DATASET}/"
    f"_dq_rules/{STAGE}/default_rules.json"
)

CUSTOM_RULES_PATH = (
    f"s3://{DATASOURCE}/{DATASET}/"
    f"_dq_rules/{STAGE}/custom_rules.json"
)

# Existing custom expectations file from the current implementation.
LEGACY_EXPECTATIONS_PATH = (
    f"s3://{DATASOURCE}/{DATASET}/"
    f"_dq_rules/{STAGE}/expectations.json"
)

RESULTS_BASE_PATH = (
    f"s3://{DATASOURCE}/{DATASET}/"
    f"_dq_results/{STAGE}"
)


# ============================================================
# CELL 2 - IMPORTS
# ============================================================

import json
import uuid
from datetime import datetime, timezone

import boto3
import great_expectations as gx
import great_expectations.expectations as gxe


# ============================================================
# CELL 3 - S3 HELPERS
# ============================================================

s3 = boto3.client("s3")


def s3_parts(s3_path):
    path = s3_path.replace("s3://", "", 1)
    bucket, key = path.split("/", 1)
    return bucket, key


def s3_exists(s3_path):
    bucket, key = s3_parts(s3_path)

    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return False
        raise


def read_json_from_s3(s3_path):
    bucket, key = s3_parts(s3_path)

    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    return json.loads(
        response["Body"].read().decode("utf-8")
    )


def write_json_to_s3(s3_path, payload):
    bucket, key = s3_parts(s3_path)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json"
    )


# ============================================================
# CELL 4 - SOURCE ADAPTER
# ============================================================
# THIS IS THE ONLY PLATFORM-SPECIFIC SECTION.
#
# SMUS:
#   sqlutils.sql(...)
#
# Databricks:
#   spark.table(...)
#
# Everything below this cell receives "df".
# ============================================================

def load_from_smus_athena():
    from sagemaker_studio import sqlutils

    return sqlutils.sql(
        f"""
        SELECT *
        FROM {CATALOG}.{TABLE}
        """,
        connection_name="default.sql"
    )


def load_from_databricks():
    return spark.table(
        f"{CATALOG}.{TABLE}"
    ).toPandas()


def load_source_dataframe():
    if SOURCE_TYPE == "smus_athena":
        return load_from_smus_athena()

    if SOURCE_TYPE == "databricks":
        return load_from_databricks()

    raise ValueError(
        f"Unsupported SOURCE_TYPE: {SOURCE_TYPE}"
    )


df = load_source_dataframe()

print("DataFrame loaded")
print("Rows:", len(df))
print("Columns:", len(df.columns))


# ============================================================
# CELL 5 - DEFAULT RULE FALLBACK
# ============================================================
# This is NOT written to S3 every run.
# It is only used if default_rules.json does not exist.
# ============================================================

DEFAULT_RULE_TEMPLATE = {
    "rules": [
        {
            "rule_id": "row-count-positive",
            "name": "Row count greater than zero",
            "dimension": "Completeness",
            "expectation_type": "expect_table_row_count_to_be_between",
            "parameters": {
                "min_value": 1
            },
            "severity": "critical",
            "enabled": True
        }
    ]
}


# ============================================================
# CELL 6 - LOAD DEFAULT RULES
# ============================================================

if s3_exists(DEFAULT_RULES_PATH):
    default_rules_data = read_json_from_s3(
        DEFAULT_RULES_PATH
    )
    DEFAULT_RULES = default_rules_data["rules"]
    print("Default rules loaded from S3")
else:
    DEFAULT_RULES = DEFAULT_RULE_TEMPLATE["rules"]

    write_json_to_s3(
        DEFAULT_RULES_PATH,
        DEFAULT_RULE_TEMPLATE
    )

    print("Default rules initialized in S3")

print("Default rules:", len(DEFAULT_RULES))


# ============================================================
# CELL 7 - LOAD CUSTOM RULES
# ============================================================
# Preferred future file:
#   custom_rules.json
#
# For the current dataset, if custom_rules.json does not exist,
# consume the existing expectations.json and normalize it.
# ============================================================

def normalize_legacy_expectation(expectation):
    meta = expectation.get("meta", {})

    return {
        "rule_id": meta.get(
            "name",
            expectation["type"]
        ),
        "name": meta.get(
            "name",
            expectation["type"]
        ),
        "dimension": meta.get(
            "dimension",
            "Unspecified"
        ),
        "expectation_type": expectation["type"],
        "parameters": expectation.get("kwargs", {}),
        "severity": meta.get(
            "severity",
            "critical"
        ),
        "enabled": True
    }


if s3_exists(CUSTOM_RULES_PATH):
    custom_rules_data = read_json_from_s3(
        CUSTOM_RULES_PATH
    )
    CUSTOM_RULES = custom_rules_data.get("rules", [])

    print("Custom rules loaded from custom_rules.json")

elif s3_exists(LEGACY_EXPECTATIONS_PATH):
    legacy_data = read_json_from_s3(
        LEGACY_EXPECTATIONS_PATH
    )

    CUSTOM_RULES = [
        normalize_legacy_expectation(expectation)
        for expectation in legacy_data.get(
            "expectations",
            []
        )
    ]

    print("Custom rules loaded from existing expectations.json")

else:
    CUSTOM_RULES = []
    print("No custom rules found")

print("Custom rules:", len(CUSTOM_RULES))


# ============================================================
# CELL 8 - COMBINE FOR EXECUTION
# ============================================================
# Storage remains separate.
# They are combined only in memory for this run.
# ============================================================

ALL_RULES = DEFAULT_RULES + CUSTOM_RULES

print("Default rules:", len(DEFAULT_RULES))
print("Custom rules:", len(CUSTOM_RULES))
print("Total rules:", len(ALL_RULES))


# ============================================================
# CELL 9 - GX CONTEXT
# ============================================================

context = gx.get_context()

print("GX version:", gx.__version__)


# ============================================================
# CELL 10 - CREATE A FRESH GX BATCH
# ============================================================
# Unique runtime names avoid collisions from previous interactive
# notebook runs. These objects are NOT persisted as our rule source.
# ============================================================

run_id = str(uuid.uuid4())

data_source = context.data_sources.add_pandas(
    name=f"dq_source_{run_id}"
)

data_asset = data_source.add_dataframe_asset(
    name=f"{DATASET}_{STAGE}_{run_id}"
)

batch_definition = (
    data_asset.add_batch_definition_whole_dataframe(
        name=f"batch_{run_id}"
    )
)

batch = batch_definition.get_batch(
    batch_parameters={"dataframe": df}
)

print("GX batch created")


# ============================================================
# CELL 11 - RULE -> GX EXPECTATION
# ============================================================

EXPECTATION_MAP = {
    "expect_table_row_count_to_be_between":
        gxe.ExpectTableRowCountToBeBetween,

    "expect_column_values_to_not_be_null":
        gxe.ExpectColumnValuesToNotBeNull,

    "expect_column_values_to_be_between":
        gxe.ExpectColumnValuesToBeBetween,
}


def build_expectation(rule):
    expectation_type = rule["expectation_type"]

    if expectation_type not in EXPECTATION_MAP:
        raise ValueError(
            f"Unsupported expectation type: {expectation_type}"
        )

    expectation_class = EXPECTATION_MAP[
        expectation_type
    ]

    return expectation_class(
        **rule["parameters"],
        meta={
            "name": rule["name"],
            "dimension": rule["dimension"],
            "severity": rule["severity"],
            "rule_id": rule["rule_id"]
        }
    )


enabled_rules = [
    rule
    for rule in ALL_RULES
    if rule.get("enabled", True)
]

expectations = [
    build_expectation(rule)
    for rule in enabled_rules
]

print("GX expectations created:", len(expectations))


# ============================================================
# CELL 12 - VALIDATE
# ============================================================
# Validate each expectation directly against the current batch.
# ============================================================

validation_results = []

for rule, expectation in zip(
    enabled_rules,
    expectations
):
    try:
        result = batch.validate(expectation)

        validation_results.append({
            "rule_id": rule["rule_id"],
            "name": rule["name"],
            "dimension": rule["dimension"],
            "severity": rule["severity"],
            "expectation_type": rule["expectation_type"],
            "success": bool(result["success"]),
            "result": result
        })

        print(
            f"{rule['rule_id']} -> "
            f"{result['success']}"
        )

    except Exception as e:
        validation_results.append({
            "rule_id": rule["rule_id"],
            "name": rule["name"],
            "dimension": rule["dimension"],
            "severity": rule["severity"],
            "expectation_type": rule["expectation_type"],
            "success": False,
            "error": f"{type(e).__name__}: {e}"
        })

        print(
            f"{rule['rule_id']} -> ERROR: "
            f"{type(e).__name__}: {e}"
        )

print("Validation completed")


# ============================================================
# CELL 13 - BUILD RESULT
# ============================================================

now = datetime.now(timezone.utc)

result_payload = {
    "run_id": run_id,
    "run_date": now.strftime("%Y-%m-%d"),
    "datasource": DATASOURCE,
    "dataset": DATASET,
    "stage": STAGE,
    "source_type": SOURCE_TYPE,
    "total_rules": len(validation_results),
    "passed_rules": sum(
        1
        for result in validation_results
        if result["success"]
    ),
    "failed_rules": sum(
        1
        for result in validation_results
        if not result["success"]
    ),
    "results": validation_results
}


# ============================================================
# CELL 14 - STORE RESULTS
# ============================================================

result_path = (
    f"{RESULTS_BASE_PATH}/"
    f"rundate={result_payload['run_date']}/"
    f"runid={run_id}/"
    f"format=json/"
    f"results.json"
)

write_json_to_s3(
    result_path,
    result_payload
)

print("Results stored:")
print(result_path)


# ============================================================
# CELL 15 - SUMMARY
# ============================================================

print("========================================")
print("DATA QUALITY RUN COMPLETED")
print("========================================")
print("Datasource:", DATASOURCE)
print("Dataset:", DATASET)
print("Stage:", STAGE)
print("Total rules:", result_payload["total_rules"])
print("Passed:", result_payload["passed_rules"])
print("Failed:", result_payload["failed_rules"])
print("Run ID:", run_id)
