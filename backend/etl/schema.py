"""
Here define schemas used for the thesis project
"""

from pyspark.sql.types import StructType, StringType, StructField, ArrayType, BooleanType

author_type = StructType([
    # Define a StructField for each field
    StructField("lastName", StringType(), True),
    StructField("foreName", StringType(), True),
    StructField("initials",  StringType(), True),
    StructField("affiliations", ArrayType(StringType()), True)
])

article_schema = StructType([
    StructField("pmid", StringType(), True),
    StructField("year", StringType(), True),
    StructField("month", StringType(), True),
    StructField("day", StringType(), True),
    StructField("title", StringType(), True),
    StructField("journalTitle", StringType(), True),
    StructField("authorList", ArrayType(author_type), True),
    StructField("validity", BooleanType(), False),
    StructField("invalidFields", ArrayType(StringType()), True),
    StructField("firstInst", StringType(), True)
])
