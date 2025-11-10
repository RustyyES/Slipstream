import great_expectations as gx
import os
import sys
import socket

def is_running_in_docker():
    try:
        # Check if we can resolve the container name
        socket.gethostbyname("urban-postgres")
        print("Detected Docker environment (uran-postgres is resolvable).")
        return True
    except socket.error:
        print("Detected Local environment (urban-postgres not resolvable).")
        return False

def validate_weather():
    print("Starting Weather Data Validation...")
    context = gx.get_context()
    
    # 1. Setup Datasource
    datasource_name = "postgres_datasource"
    
    # Determine Host/Port based on environment
    if is_running_in_docker():
        host = "urban-postgres"
        port = "5432"
    else:
        host = "localhost"
        port = "5433"
        
    connection_string = f"postgresql+psycopg2://admin:password123@{host}:{port}/urban_analytics"
    print(f"Connecting to Postgres at: {host}:{port}")

    try:
        datasource = context.data_sources.get(datasource_name)
    except KeyError:
        datasource = context.data_sources.add_sql(
            name=datasource_name,
            connection_string=connection_string,
        )

    # 2. Setup Asset
    asset_name = "weather_summary"
    try:
        asset = datasource.get_asset(asset_name)
    except (LookupError, KeyError):
        asset = datasource.add_table_asset(
            name=asset_name,
            table_name="weather_summary",
            schema_name="serving"
        )

    # 3. Define Expectations
    suite_name = "weather_summary_suite"
    try:
        suite = context.suites.get(suite_name)
    except (gx.exceptions.DataContextError, KeyError):
        suite = context.suites.add(
            gx.core.expectation_suite.ExpectationSuite(name=suite_name)
        )

    batch_request = asset.build_batch_request()
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    print("Defining Expectations...")
    # Critical columns
    validator.expect_column_values_to_not_be_null("city")
    validator.expect_column_values_to_not_be_null("avg_temp")
    
    # Temperature (Physics check)
    validator.expect_column_values_to_be_between("avg_temp", min_value=-50, max_value=60)
    
    # Humidity (Percentage check)
    validator.expect_column_values_to_be_between("avg_humidity", min_value=0, max_value=100)

    # Save suite
    validator.save_expectation_suite(discard_failed_expectations=False)

    # 4. Run Checkpoint
    print("Running Validation Checkpoint...")
    checkpoint = context.add_or_update_checkpoint(
        name="weather_checkpoint",
        validations=[{"batch_request": batch_request, "expectation_suite_name": suite_name}],
    )

    checkpoint_result = checkpoint.run()

    # 5. Report Results
    if checkpoint_result["success"]:
        print("\n✅ VALIDATION SUCCEEDED!")
    else:
        print("\n❌ VALIDATION FAILED!")
        
    for validation_result in checkpoint_result.run_results.values():
        stats = validation_result["validation_result"]["statistics"]
        print(f"  Success Percent: {stats['success_percent']}%")
        print(f"  Successful: {stats['successful_expectations']}")
        print(f"  Unsuccessful: {stats['unsuccessful_expectations']}")
    
    if not checkpoint_result["success"]:
        sys.exit(1)

if __name__ == "__main__":
    validate_weather()
