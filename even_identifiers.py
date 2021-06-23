import modelop.monitors.volumetrics as volumetrics
import modelop.schema.infer as infer
import modelop.utils as utils

logger = utils.configure_logger()

MONITORING_PARAMETERS = {}

# modelop.init
def init(job_json):
    """A function to extract input schema from job JSON.

    Args:
        job_json (str): job JSON in a string format.
    """
    # Extract input schema from job JSON
    input_schema_definition = infer.extract_input_schema(job_json)

    logger.info("Input schema definition: %s", input_schema_definition)

    # Get monitoring parameters from schema
    global MONITORING_PARAMETERS
    MONITORING_PARAMETERS = infer.set_monitoring_parameters(
        schema_json=input_schema_definition, check_schema=True
    )


# modelop.metrics
def metrics(df):
    if not MONITORING_PARAMETERS["identifier_columns"]:
        raise ValueError("No identifier columns found in extended schema")

    id_column = MONITORING_PARAMETERS["identifier_columns"][0]
    even_identifiers = df[df[id_column] % 2 == 0]
    count_even_identifiers = even_identifiers.shape[0]

    test_result = {
        "test_name": "Count of Even Identifiers",
        "test_category": "volumetrics",
        "test_type": "count",
        "test_id": "volumetrics_count_even",
        "values": {"even_count": count_even_identifiers},
    }

    result = {
        # Flat top level metric of count (for MLCs)
        "even_count": count_even_identifiers,
        # Complete test results
        "volumetrics": [test_result],
    }
    yield result


if __name__ == "__main__":
    import pandas

    # global MONITORING_PARAMETERS
    MONITORING_PARAMETERS["identifier_columns"] = ["id"]

    df = pandas.read_json("df_sample_scored.json", lines=True)

    from pprint import pprint

    pprint(next(metrics(df)))
