from html import entities
import os
from importlib_metadata import metadata
import json
import pandas as pd
import numpy as np
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from pyspark.sql.types import StructType, StringType, StructField, ArrayType

gep_type = StructType([
    # Define a StructField for each field
    StructField('instFull', StringType(), True),
    StructField('gepEntities', ArrayType(StringType()), True),
])


spark = sparknlp.start()

MODEL_NAME = "onto_100"

df = spark.read.parquet("../data/600k.parquet")

df_valid = df.filter(df.validity == True)

df_inst = df_valid.select(
    'firstInst')
print(df_inst.count())


documentAssembler = DocumentAssembler() \
    .setInputCol('firstInst') \
    .setOutputCol('document')

tokenizer = Tokenizer() \
    .setInputCols(['document']) \
    .setOutputCol('token')

# ner_dl and onto_100 model are trained with glove_100d, so the embeddings in
# the pipeline should match
if (MODEL_NAME == "ner_dl") or (MODEL_NAME == "onto_100"):
    embeddings = WordEmbeddingsModel.pretrained('glove_100d').setInputCols(
        ["document", 'token']).setOutputCol("embeddings")

# Bert model uses Bert embeddings
elif MODEL_NAME == "ner_dl_bert":
    embeddings = BertEmbeddings.pretrained(name='bert_base_cased', lang='en') \
        .setInputCols(['document', 'token']) \
        .setOutputCol('embeddings')

ner_model = NerDLModel.pretrained(MODEL_NAME, 'en') \
    .setInputCols(['document', 'token', 'embeddings']) \
    .setOutputCol('ner')

ner_converter = NerConverter() \
    .setInputCols(['document', 'token', 'ner']) \
    .setOutputCol('ner_chunk')

nlp_pipeline = Pipeline(stages=[
    documentAssembler,
    tokenizer,
    embeddings,
    ner_model,
    ner_converter
])


empty_df = spark.createDataFrame([['']]).toDF('firstInst')
pipeline_model = nlp_pipeline.fit(empty_df)
# df = spark.createDataFrame(pd.DataFrame({'text': text_list}))
result = pipeline_model.transform(df_inst)

res_geo = result.select(
    'firstInst', result['ner_chunk'])

# .where(array_contains(col('ner_chunk'), "GPE"))


def get_gpe_map(input_s):
    """choose gpe"""
    try:
        org_entity = input_s["ner_chunk"]
        entity_final = []
        for entity in org_entity:
            if(entity["metadata"]["entity"] == "GPE"):
                entity_final.append(entity["result"])
                # print(entity["result"])
        return input_s["firstInst"], entity_final
    except (IndexError, AttributeError, ValueError):
        return input_s["firstInst"], None


final = res_geo.rdd.map(get_gpe_map).toDF(schema=gep_type)

final.write.format('parquet') \
    .mode('overwrite') \
    .save(os.path.join("../data", "gep_entity"))


# for pos, row in res_geo.foreach():
#     try:
#         org_entity = row["ner_chunk"]
#         entity_final = []
#         for entity in org_entity:
#             if(entity["metadata"]["entity"] == "GPE"):
#                 entity_final.append(entity["result"])
#         df_res.concat(row["firstInst"], entity_final)
#     except (IndexError, AttributeError, ValueError):
#         df_res.concat(row["firstInst"], None)

# logging.error(input_string)
# logging.error(e)

# print(type(res_json))

# with open("./testjson.json", 'w', encoding='UTF-8') as f:
#     json.dump(dump_res_json, f, indent=4)
# |-- firstInst: string (nullable = true)
#  |-- document: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- annotatorType: string (nullable = true)
#  |    |    |-- begin: integer (nullable = false)
#  |    |    |-- end: integer (nullable = false)
#  |    |    |-- result: string (nullable = true)
#  |    |    |-- metadata: map (nullable = true)
#  |    |    |    |-- key: string
#  |    |    |    |-- value: string (valueContainsNull = true)
#  |    |    |-- embeddings: array (nullable = true)
#  |    |    |    |-- element: float (containsNull = false)
#  |-- token: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- annotatorType: string (nullable = true)
#  |    |    |-- begin: integer (nullable = false)
#  |    |    |-- end: integer (nullable = false)
#  |    |    |-- result: string (nullable = true)
#  |    |    |-- metadata: map (nullable = true)
#  |    |    |    |-- key: string
#  |    |    |    |-- value: string (valueContainsNull = true)
#  |    |    |-- embeddings: array (nullable = true)
#  |    |    |    |-- element: float (containsNull = false)
#  |-- embeddings: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- annotatorType: string (nullable = true)
#  |    |    |-- begin: integer (nullable = false)
#  |    |    |-- end: integer (nullable = false)
#  |    |    |-- result: string (nullable = true)
#  |    |    |-- metadata: map (nullable = true)
#  |    |    |    |-- key: string
#  |    |    |    |-- value: string (valueContainsNull = true)
#  |    |    |-- embeddings: array (nullable = true)
#  |    |    |    |-- element: float (containsNull = false)
#  |-- ner: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- annotatorType: string (nullable = true)
#  |    |    |-- begin: integer (nullable = false)
#  |    |    |-- end: integer (nullable = false)
#  |    |    |-- result: string (nullable = true)
#  |    |    |-- metadata: map (nullable = true)
#  |    |    |    |-- key: string
#  |    |    |    |-- value: string (valueContainsNull = true)
#  |    |    |-- embeddings: array (nullable = true)
#  |    |    |    |-- element: float (containsNull = false)
#  |-- ner_chunk: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- annotatorType: string (nullable = true)
#  |    |    |-- begin: integer (nullable = false)
#  |    |    |-- end: integer (nullable = false)
#  |    |    |-- result: string (nullable = true)
#  |    |    |-- metadata: map (nullable = true)
#  |    |    |    |-- key: string
#  |    |    |    |-- value: string (valueContainsNull = true)
#  |    |    |-- embeddings: array (nullable = true)
#  |    |    |    |-- element: float (containsNull = false)

# result.write.format('parquet') \
#     .mode('overwrite') \
#     .save('./', 'test')
