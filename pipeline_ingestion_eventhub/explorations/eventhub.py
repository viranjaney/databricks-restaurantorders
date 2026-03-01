from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

EH_NAMESPACE=spark.conf.get("eh.namespace")
EH_NAME=spark.conf.get("eh.name")
EH_CONN_STR=spark.conf.get("eh.connectionString")

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : "60000",
  "kafka.session.timeout.ms" : "30000",
  "maxOffsetsPerTrigger"     : "50000",
  "failOnDataLoss"           : "true",
  "startingOffsets"          : "earliest"
}

@dp.table(name="orders", table_properties={"quality": "bronze"})
def orders():
    df_raw = (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )


    orders_schema = '''STRUCT<created_at: STRING, customer_id: STRING, items: ARRAY<STRUCT<category: STRING, item_id: STRING, name: STRING, quantity: BIGINT, subtotal: DOUBLE, unit_price: DOUBLE>>, order_id: STRING, order_status: STRING, order_type: STRING, payment_method: STRING, restaurant_id: STRING, timestamp: STRING, total_amount: DOUBLE>'''

    df_parsed = (
        df_raw
        .withColumn("key_str", col("key").cast("string"))
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("data", from_json("value_str", orders_schema))
        .select("data.*")
        .withColumnRenamed("timmestamp", "order_timestamp")
    )

    return df_parsed
