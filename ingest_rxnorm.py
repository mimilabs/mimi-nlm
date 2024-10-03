# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/nlm/src/rxnorm/"


# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

def is_column_all_null(df, column_name):
    return df.where(F.col(column_name).isNotNull()).count() == 0

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.nlm.rxnconso;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.nlm.rxnrel;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.nlm.rxnsat;

# COMMAND ----------

header_map = {"rxnrel": ["rxcui1", 
        "rxaui1", 
        "stype1",
        "rel",
        "rxcui2",
        "rxaui2",
        "stype2",
        "rela",
        "rui",
        "srui",
        "sab",
        "sl",
        "dir",
        "rg",
        "supress",
        "cvf"], 
    "rxnconso": ["rxcui", 
        "lat", 
        "ts", 
        "lui", 
        "stt", 
        "sui", 
        "ispref", 
        "rxaui", 
        "saui", 
        "scui",
        "sdui",
        "sab", 
        "tty", 
        "code", 
        "str", 
        "srl", 
        "suppress", 
        "cvf"],
    "rxnsat": [
        "rxcui",
        "lui",
        "sui",
        "rxaui",
        "stype",
        "code",
        "atui",
        "satui",
        "atn",
        "sab",
        "atv",
        "suppress",
        "cvf"
    ]}

# COMMAND ----------

tables = ["rxnrel", "rxnconso", "rxnsat"]

# COMMAND ----------

for table in tables:
    filepath_lst = sorted([filepath 
                           for filepath in Path(volumepath).rglob(f"{table.upper()}.RRF")],
                        reverse=True)
    files_exist = {}
    if spark.catalog.tableExists(f"mimi_ws_1.nlm.{table}"):
        files_exist = {x["mimi_src_file_name"] 
                    for x in (spark.read.table(f"mimi_ws_1.nlm.{table}")
                                .select("mimi_src_file_name")
                                .distinct()
                                .collect())}
    for filepath in filepath_lst:
        if not ('RxNorm_full_prescribe' in filepath.parents[1].stem):
            # pass the weekly files for now
            continue
        mimi_src_file_date = datetime.strptime(filepath.parents[1].stem[-8:], 
                                               '%m%d%Y').date()
        mimi_src_file_name = filepath.parents[1].stem + '/rrf/' + filepath.name
        if mimi_src_file_name in files_exist:
            continue

        df = (spark.read.option("delimiter", "|").option("header", "false")
                .csv(str(filepath)))
        for idx, column_name in enumerate(header_map[table]):
            df = df.withColumnRenamed(f"_c{idx}", column_name)

        df = (df.withColumn("mimi_src_file_date", F.lit(mimi_src_file_date))
                .withColumn("mimi_src_file_name", F.lit(mimi_src_file_name))
                .withColumn("mimi_dlt_load_date", F.lit(datetime.today().date())))
        # Get list of columns that are entirely null
        null_columns = [column for column in df.columns 
                        if is_column_all_null(df, column)]

        # Drop the null columns
        df = df.drop(*null_columns)
        (df.write.mode('overwrite')
            .option('mergeSchema', 'true')
            .option('replaceWhere', f"mimi_src_file_name = '{mimi_src_file_name}'")
            .saveAsTable(f"mimi_ws_1.nlm.{table}"))

# COMMAND ----------

#df_paths = spark.read.table('mimi_ws_1.sandbox.rxnorm_paths')
#df_paths.write.mode('overwrite').saveAsTable('mimi_ws_1.nlm.rxnorm_paths')

# COMMAND ----------


