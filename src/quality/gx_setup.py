import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.exceptions import DataContextError
import os
import sys

# Ensure we can import from src if needed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

def setup_gx():
    print("Initializing Great Expectations Data Context...")
    context = gx.get_context()

    # Define the connection string to the running Postgres container
    # Note: connect to localhost:5433 because that's the mapped port for urban-postgres
    connection_string = "postgresql+psycopg2://admin:password123@localhost:5433/urban_analytics"
    
    datasource_name = "postgres_datasource"
    
    try:
        # Check if datasource exists
        context.get_datasource(datasource_name)
        print(f"Datasource '{datasource_name}' already exists.")
    except ValueError:
        print(f"Adding Datasource '{datasource_name}'...")
        context.sources.add_sql(
            name=datasource_name,
            connection_string=connection_string,
        )
        print(f"Datasource '{datasource_name}' added successfully.")

    # List available assets (tables) to verify connection
    datasource = context.get_datasource(datasource_name)
    # We want to add the weather_summary table as an asset
    asset_name = "weather_summary"
    table_name = "serving.weather_summary"
    
    try:
        asset = datasource.get_asset(asset_name)
        print(f"Asset '{asset_name}' already exists.")
    except LookupError:
        print(f"Adding Asset '{asset_name}' for table '{table_name}'...")
        asset = datasource.add_table_asset(
            name=asset_name,
            table_name="weather_summary",
            schema_name="serving"
        )
        print(f"Asset '{asset_name}' added successfully.")

    return context

if __name__ == "__main__":
    try:
        context = setup_gx()
        print("\nGreat Expectations Setup Complete!")
        print(context)
    except Exception as e:
        print(f"\nSetup Failed: {e}")
        sys.exit(1)
