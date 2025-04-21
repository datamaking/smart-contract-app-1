Below is a complete, enterprise-grade PySpark application that performs schema validation between a source and target table using a "smart contract" (interpreted as a configurable schema definition). The implementation includes proper logging, exception handling, and a modular design following best practices. The project structure and code are provided to meet the requirements.

The "smart contract" is implemented as a JSON configuration file defining the expected schema, which the source data must conform to before being written to the target table. The application uses a simple yet extensible design, with the potential to incorporate patterns like Strategy or Configuration for future enhancements.

---

### Project Structure

```
project/
├── config/
│   └── smart_contract.json
├── src/
│   ├── main.py
│   ├── utils.py
│   └── schema_validator.py
├── logs/
│   └── app.log
└── requirements.txt
```

- **`config/smart_contract.json`**: Defines the expected schema for validation.
- **`src/main.py`**: Entry point of the application, orchestrating the workflow.
- **`src/utils.py`**: Utility functions, including schema loading.
- **`src/schema_validator.py`**: Schema validation logic.
- **`logs/app.log`**: Log file for tracking execution and errors.
- **`requirements.txt`**: Lists dependencies (PySpark).

---

### Code Implementation

#### `config/smart_contract.json`
This file represents the "smart contract," specifying the expected schema with column names, data types, and nullability.

```json
{
  "fields": [
    {
      "metadata": {},
      "name": "id",
      "nullable": false,
      "type": "integer"
    },
    {
      "metadata": {},
      "name": "name",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "age",
      "nullable": true,
      "type": "integer"
    }
  ],
  "type": "struct"
}
```

#### `src/utils.py`
Contains utility functions, such as loading the schema from the JSON file.

```python
import json
from pyspark.sql.types import StructType

def load_schema_from_json(json_path):
    """Load a PySpark StructType schema from a JSON file."""
    with open(json_path, 'r') as f:
        schema_dict = json.load(f)
    return StructType.fromJson(schema_dict)
```

#### `src/schema_validator.py`
Implements the schema validation logic, ensuring the source schema meets the requirements defined in the smart contract. It allows extra columns in the source but enforces the presence and properties of required fields.

```python
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
                    nullability_mismatches.append((field, expected_fields[field].nullable, actual_fields[field].nullable))
        
        # Log errors if validation fails
        if missing_fields or type_mismatches or nullability_mismatches:
            for field in missing_fields:
                logging.error(f"Missing required field: {field}")
            for field, expected_type, actual_type in type_mismatches:
                logging.error(f"Type mismatch for field {field}: expected {expected_type}, got {actual_type}")
            for field, expected_nullable, actual_nullable in nullability_mismatches:
                logging.error(f"Nullability mismatch for field {field}: expected {expected_nullable}, got {actual_nullable}")
            return False
        else:
            logging.info("Schema validation passed.")
            return True
```

#### `src/main.py`
The main application script that ties everything together: loading the schema, reading data, validating it, and writing to the target table if valid.

```python
import logging
from pyspark.sql import SparkSession
from src.utils import load_schema_from_json
from src.schema_validator import SchemaValidator

def main():
    """Main function to perform schema validation and data processing."""
    # Configure logging
    logging.basicConfig(
        filename='logs/app.log',
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s'
    )
    
    try:
        # Load the expected schema from the smart contract
        logging.info("Loading schema from smart contract.")
        expected_schema = load_schema_from_json('config/smart_contract.json')
        
        # Initialize SparkSession
        logging.info("Initializing SparkSession.")
        spark = SparkSession.builder.appName("SchemaValidation").getOrCreate()
        
        # Read source data (example: Parquet file)
        logging.info("Reading source data.")
        source_df = spark.read.format("parquet").load("path/to/source")
        
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
        spark.stop()

if __name__ == "__main__":
    main()
```

#### `requirements.txt`
Specifies the PySpark dependency.

```plain
pyspark==3.2.0
```

---

### Explanation of Design Choices

1. **Schema Validation**:
   - The `SchemaValidator` class checks if the source DataFrame contains all required fields from the smart contract with matching data types and nullability.
   - It allows extra columns in the source for flexibility, logging errors only for missing or mismatched required fields.

2. **Smart Contract**:
   - Implemented as a JSON file (`smart_contract.json`) defining the expected schema, making it configurable and decoupled from the code.

3. **Logging**:
   - Uses Python’s `logging` module to write detailed logs to `logs/app.log`, including timestamps and error details for debugging and monitoring.

4. **Exception Handling**:
   - Wraps the main logic in a `try-except` block to catch and log any errors, ensuring the application fails gracefully.

5. **Design Patterns**:
   - **Configuration Pattern**: The smart contract JSON serves as a configuration, allowing schema rules to be modified without code changes.
   - **Modular Design**: Logic is split into separate modules (`utils.py`, `schema_validator.py`) for maintainability and reusability.

6. **Enterprise-Grade Features**:
   - Proper separation of concerns, configurable schema, detailed logging, and robust error handling make it suitable for production use.

---

### How to Use

1. **Setup**:
   - Install PySpark: `pip install -r requirements.txt`.
   - Replace `"path/to/source"` in `main.py` with the actual source data path.
   - Adjust the target table name (`target_table`) as needed.

2. **Run**:
   - Execute `python src/main.py` or use `spark-submit` for a distributed environment.

3. **Monitor**:
   - Check `logs/app.log` for execution details and any validation errors.

This implementation provides a solid foundation for schema validation in PySpark, with room to extend functionality (e.g., transformation rules, stricter validation) as requirements evolve.