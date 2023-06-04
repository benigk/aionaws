
import os

import sparknlp

from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from sparknlp.annotator import (
    BertEmbeddings, NerConverter, NerDLModel, Tokenizer, WordEmbeddingsModel
)
from sparknlp.base import DocumentAssembler



def gep_type() -> StructType:
    """Define a StructField for each field"""

    return StructType([
        StructField("instFull", StringType(), True),
        StructField("gepEntities", ArrayType(StringType()), True),
    ])


def get_gpe_map(input_s):
    """choose gpe"""
    try:
        org_entity = input_s["ner_chunk"]
        entity_final = []
        for entity in org_entity:
            if entity["metadata"]["entity"] == "GPE":
                entity_final.append(entity["result"])
                # print(entity["result"])
        return input_s["firstInst"], entity_final
    except (IndexError, AttributeError, ValueError):
        return input_s["firstInst"], None


def main():
    spark = sparknlp.start()
    MODEL_NAME = "onto_100"
    df = spark.read.parquet("../data/600k.parquet")
    df_valid = df.filter(df.validity is True)
    df_inst = df_valid.select("firstInst")


    documentAssembler = DocumentAssembler().setInputCol("firstInst").setOutputCol("document")
    tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("token")

    # ner_dl and onto_100 model are trained with glove_100d, so the embeddings in
    # the pipeline should match
    if MODEL_NAME in {"ner_dl", "onto_100"}:
        embeddings = WordEmbeddingsModel.pretrained(
            "glove_100d"
        ).setInputCols(
            ["document", "token"]
        ).setOutputCol("embeddings")


    # Bert model uses Bert embeddings
    elif MODEL_NAME == "ner_dl_bert":
        embeddings = BertEmbeddings.pretrained(
            name="bert_base_cased", lang="en"
        ).setInputCols(
            ["document", "token"]
        ).setOutputCol("embeddings")


    ner_model = NerDLModel.pretrained(
        MODEL_NAME, "en"
    ).setInputCols(
        ["document", "token", "embeddings"]
    ).setOutputCol("ner")

    ner_converter = NerConverter().setInputCols(
        ["document", "token", "ner"]
    ).setOutputCol("ner_chunk")

    nlp_pipeline = Pipeline(stages=[
        documentAssembler,
        tokenizer,
        embeddings,
        ner_model,
        ner_converter
    ])


    empty_df = spark.createDataFrame([[""]]).toDF("firstInst")
    pipeline_model = nlp_pipeline.fit(empty_df)
    # df = spark.createDataFrame(pd.DataFrame({"text": text_list}))
    result = pipeline_model.transform(df_inst)
    res_geo = result.select("firstInst", result["ner_chunk"])
    # .where(array_contains(col("ner_chunk"), "GPE"))

    final = res_geo.rdd.map(get_gpe_map).toDF(schema=gep_type())
    final.write.format("parquet").mode("overwrite").save(os.path.join("../data", "gep_entity"))
