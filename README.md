# Even identifiers volumetrics model

This is an example model with a custom volumetrics monitoring test.

The model takes an input dataframe and counts the number of even identifiers based on the first identifier column specified by the base model's extended Avro schema.

`df_sample_scored.json` in this repo is an example input file, and it is copied from the [German credit model](https://github.com/merhi-odg/german_credit_python/tree/extended_schema).

To test this monitor, follow these steps:
- Import this repo into MOC and create a snapshot. Attach a runtime to the snapshot that has the [monitoring package](https://github.com/modelop/moc_monitors) installed.
- Import the [German credit model](https://github.com/merhi-odg/german_credit_python/tree/extended_schema) (`extended_schema` branch) and create a snapshot.
- In the German credit snapshot, add this monitor. The association role `PERFORMANCE_MODEL` should already be selected, and for the sample data asset add the `df_sample_scored.json` asset that's already stored in the German credit model.
- Run the monitor either via a schedule or manual run, and the test result should be a "Count of Even Identifiers" table with an `even_count` column

