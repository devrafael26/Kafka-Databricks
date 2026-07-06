checkpoint_path = "/Volumes/workspace/default/checkpoint_volume/raw_orders"
base_path = "/Volumes/workspace/default/checkpoint_volume"

# Credenciais do Databricks Secrets
kafka_username = dbutils.secrets.get(scope="confluent", key="username")
kafka_password = dbutils.secrets.get(scope="confluent", key="password")

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
    f"required username='{kafka_username}' password='{kafka_password}';"
)

df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("subscribe", "sample_data_orders")
        .option("startingOffsets", "earliest")
        .load()
)

query_raw = df.selectExpr(
    "CAST(key AS STRING) as key",
    "value"  
) \
.writeStream \
.format("delta") \
.option("checkpointLocation", checkpoint_path) \
.outputMode("append") \
.trigger(availableNow=True) \
.toTable("raw_orders")