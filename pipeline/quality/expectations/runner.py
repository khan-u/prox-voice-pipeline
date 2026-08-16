"""Run a Great Expectations suite against a Spark DataFrame.

Uses an ephemeral GX context so validation needs no on-disk project — the suite
is built in-process, run over a whole-dataframe batch, and the result returned.
This is the shared harness the silver and gold expectation suites call.
"""
import great_expectations as gx


def run_suite(df, suite_name, expectations):
    """Validate `df` against `expectations`; return the GX result object.

    `expectations` is a list of gx.expectations.* instances. `result.success`
    is True only if every expectation passes.
    """
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_spark(f"{suite_name}_source")
    asset = source.add_dataframe_asset(f"{suite_name}_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe("batch")

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for expectation in expectations:
        suite.add_expectation(expectation)

    validation = context.validation_definitions.add(
        gx.ValidationDefinition(
            data=batch_definition, suite=suite, name=f"{suite_name}_validation"
        )
    )
    return validation.run(batch_parameters={"dataframe": df})
