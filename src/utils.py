import json
from pyspark.sql.types import StructType

def load_schema_from_json(json_path):
    """Load a PySpark StructType schema from a JSON file."""
    with open(json_path, 'r') as f:
        schema_dict = json.load(f)
    return StructType.fromJson(schema_dict)