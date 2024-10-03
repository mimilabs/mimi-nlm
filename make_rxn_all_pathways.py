# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

max_mimi_src_file_date = (spark.read.table('mimi_ws_1.nlm.rxnconso')
                .select(F.max('mimi_src_file_date').alias('max_mimi_src_file_date'))
                .collect())[0]['max_mimi_src_file_date']

# COMMAND ----------

paths = spark.read.table('mimi_ws_1.nlm.rxnorm_paths').toPandas()['path']
rxnconso = (spark.read.table('mimi_ws_1.nlm.rxnconso')
                .filter(F.col('mimi_src_file_date') == max_mimi_src_file_date)
                .select('rxcui', 'tty')
                .dropDuplicates())
rxnrel = (spark.read.table('mimi_ws_1.nlm.rxnrel')
                .filter(F.col('mimi_src_file_date') == max_mimi_src_file_date)
                .select('rxcui1', 'rxcui2')
                .dropDuplicates())

# COMMAND ----------

df = (rxnrel.join(rxnconso, on=(rxnrel.rxcui1 == rxnconso.rxcui), how = 'left')
        .withColumnRenamed('tty', 'tty1')
        .drop('rxcui')
        .join(rxnconso, on=(rxnrel.rxcui2 == rxnconso.rxcui), how = 'left')
        .withColumnRenamed('tty', 'tty2')
        .drop('rxcui'))

# COMMAND ----------

df_all_pathways = None

for i, path in enumerate(paths):
    df_path = None
    path_str = ' => '.join(path)
    print(path_str, '...')

    for j, (source, target) in enumerate(zip(path[:-1], path[1:])):
        is_first_pair = (j == 0)
        is_last_pair = (j == len(path)-2)
        prefix = 'link'
        if is_last_pair:
            prefix = 'target'
        
        df_tmp = df.filter((df.tty1 == source) & (df.tty2 == target))
        
        if is_first_pair:
            df_path = (df_tmp.withColumnRenamed('rxcui1', 'source_rxcui')
                            .withColumnRenamed('tty1', 'source_tty'))
        else:
            df_path = (df_path.join(df_tmp,
                                   on=(df_path.link_rxcui == df_tmp.rxcui1),
                                   how='inner')
                            .drop('rxcui1', 'tty1', 'link_rxcui', 'link_tty'))
        
        df_path = (df_path.withColumnRenamed('rxcui2', f'{prefix}_rxcui')
                            .withColumnRenamed('tty2', f'{prefix}_tty'))
        
    df_path = (df_path.withColumn('path', F.lit(path_str))
                .select('source_rxcui', 
                        'source_tty', 
                        'target_rxcui', 
                        'target_tty', 
                        'path'))

    if i == 0:
        df_all_pathways = df_path
    else:
        df_all_pathways = df_all_pathways.union(df_path)


# COMMAND ----------

rxcui2str = (spark.read.table('mimi_ws_1.nlm.rxnconso')
                .filter(F.col('mimi_src_file_date') == max_mimi_src_file_date)
                .select('rxcui', 'str')
                .dropDuplicates())

# COMMAND ----------

df_all_pathways = (df_all_pathways.join(rxcui2str, 
                     on=(df_all_pathways.source_rxcui == rxcui2str.rxcui),
                     how='left')
                        .withColumnRenamed('str', 'source_name')
                        .drop('rxcui')
                        .join(rxcui2str,
                    on=(df_all_pathways.target_rxcui == rxcui2str.rxcui),
                    how='left')
                        .withColumnRenamed('str', 'target_name')
                        .drop('rxcui')) 

# COMMAND ----------

df_all_pathways = df_all_pathways.select('source_rxcui', 
                                        'source_name',
                                        'source_tty',
                                        'target_rxcui',
                                        'target_name',
                                        'target_tty',
                                        'path')

# COMMAND ----------

df_all_pathways.write.mode('overwrite').saveAsTable('mimi_ws_1.nlm.rxn_all_pathways')

# COMMAND ----------


