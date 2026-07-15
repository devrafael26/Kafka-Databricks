# Databricks notebook source

# COMMAND ----------

checkpoint_path = "/Volumes/workspace/default/checkpoint_volume/raw_orders"

base_path = "/Volumes/workspace/default/checkpoint_volume"


# COMMAND ----------

# Credenciais do Databricks Secrets

kafka_username = dbutils.secrets.get(
    scope="confluent",
    key="username"
)

kafka_password = dbutils.secrets.get(
    scope="confluent",
    key="password"
)


# COMMAND ----------

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
    f"required username='{kafka_username}' password='{kafka_password}';"
)


# COMMAND ----------

df = (
    spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092"
        )
        .option(
            "kafka.security.protocol",
            "SASL_SSL"
        )
        .option(
            "kafka.sasl.mechanism",
            "PLAIN"
        )
        .option(
            "kafka.sasl.jaas.config",
            jaas_config
        )
        .option(
            "subscribe",
            "sample_data_orders"
        )
        .option(
            "startingOffsets",
            "earliest"
        )
        .load()
)


# COMMAND ----------

(
    df.writeStream
      .format("delta")
      .option(
          "checkpointLocation",
          checkpoint_path
      )
      .trigger(once=True)
      .outputMode("append")
      .table("raw_orders")
)