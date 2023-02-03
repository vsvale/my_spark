from pyspark.sql.functions import greatest, when, col, lit

def get_greatest_int(df):
    df_transformed = df.select(greatest("id","build_number","version","user_id").alias("highest_number"))
    return df_transformed

def get_rate(df):
    df_transformed = df.withColumn("rank",when(col("manufacturer")=="Xiamomi",lit("low")).otherwise(lit("normal")))
    return df_transformed
