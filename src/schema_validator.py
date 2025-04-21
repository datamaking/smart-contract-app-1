import logging
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField


class SchemaValidator:
    """Validates a DataFrame's schema against an expected schema."""

    def __init__(self, expected_schema):
        self.expected_schema = expected_schema

    def validate(self, df: DataFrame) -> bool:
        """
        Validate the DataFrame schema against the expected schema.
        Returns True if valid, False otherwise, logging discrepancies.
        """
        expected_fields = {f.name: f for f in self.expected_schema.fields}
        actual_fields = {f.name: f for f in df.schema.fields}

        missing_fields = []
        type_mismatches = []
        nullability_mismatches = []

        # Check for missing fields or mismatches
        for field in expected_fields:
            if field not in actual_fields:
                missing_fields.append(field)
            else:
                if expected_fields[field].dataType != actual_fields[field].dataType:
                    type_mismatches.append((field, expected_fields[field].dataType, actual_fields[field].dataType))
                if expected_fields[field].nullable != actual_fields[field].nullable:
                    nullability_mismatches.append(
                        (field, expected_fields[field].nullable, actual_fields[field].nullable))

        # Log errors if validation fails
        if missing_fields or type_mismatches or nullability_mismatches:
            for field in missing_fields:
                logging.error(f"Missing required field: {field}")
            for field, expected_type, actual_type in type_mismatches:
                logging.error(f"Type mismatch for field {field}: expected {expected_type}, got {actual_type}")
            for field, expected_nullable, actual_nullable in nullability_mismatches:
                logging.error(
                    f"Nullability mismatch for field {field}: expected {expected_nullable}, got {actual_nullable}")
            return False
        else:
            logging.info("Schema validation passed.")
            return True