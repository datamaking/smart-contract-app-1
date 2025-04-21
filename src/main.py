import logging
from pyspark.sql import SparkSession
from src.utils import load_schema_from_json
from src.schema_validator import SchemaValidator
from pyspark.sql.types import *


def main():
    """Main function to perform schema validation and data processing."""
    # Configure logging
    logging.basicConfig(
        filename='/home/datamaking/PycharmProjects/smart-contract-app/logs/app.log',
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s'
    )

    spark = None
    try:
        # Load the expected schema from the smart contract
        logging.info("Loading schema from smart contract.")
        expected_schema = load_schema_from_json('/home/datamaking/PycharmProjects/smart-contract-app/config/smart_contract.json')

        # Initialize SparkSession
        logging.info("Initializing SparkSession.")
        spark = SparkSession.builder.appName("SchemaValidation").getOrCreate()

        # Read source data (example: Parquet file)
        logging.info("Reading source data.")
        #source_df = spark.read.format("parquet").load("path/to/source")
        employee_data_path = "file:///home/datamaking/PycharmProjects/smart-contract-app/data"
        employee_data_parqet_path = "file:///home/datamaking/PycharmProjects/smart-contract-app/data_parquet"
        #source_df = spark.read.format("csv").option("header", "true").load(employee_data_path)

        source_schema = StructType([
            StructField('id', IntegerType(), False),
            StructField('name', StringType(), True),
            StructField('age', IntegerType(), True)]
        )

        print(expected_schema)
        print("\n")
        print(source_schema)
        '''source_df = (spark.read.format("json")
                     .option("header", "true")
                     .schema(source_schema)
                     .load(employee_data_path))'''

        #source_df = spark.read.schema(source_schema).option("enforceSchema", "true").parquet(employee_data_parqet_path)
        #source_df.write.parquet(employee_data_parqet_path, mode="overwrite")

        #.option("inferSchema", "false")
        #.option("enforceSchema", "true")

        data = [
            {"id": 1, "name": "sivan", "age": 80},
            {"id": 2, "name": "ram", "age": 70},
            {"id": 3, "name": "sam", "age": 65}
            ]

        source_df = spark.createDataFrame(data, schema=source_schema)

        print(source_df.columns)
        print(source_df.printSchema())

        for dtype in source_df.dtypes:
            print(type(dtype))
            print(dir(dtype))
            print("\n")
            print(dtype)

        # Perform schema validation
        logging.info("Starting schema validation.")
        validator = SchemaValidator(expected_schema)
        if validator.validate(source_df):
            # Write to target table if validation passes
            logging.info("Writing validated data to target table.")
            source_df.write.mode("append").saveAsTable("target_table")
            logging.info("Data successfully written to target table.")
        else:
            logging.error("Schema validation failed. Data not written to target.")
            raise ValueError("Schema validation failed. Check logs for details.")

    except Exception as e:
        logging.exception(f"An error occurred: {str(e)}")
        raise

    finally:
        # Clean up SparkSession
        logging.info("Stopping SparkSession.")
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()