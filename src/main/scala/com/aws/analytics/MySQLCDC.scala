package com.aws.analytics

import com.aws.analytics.conf.Config
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

object MySQLCDC {
  def createCDCSource(params: Config): MySqlSource[String] = {
    var startPos = StartupOptions.initial()
    if (params.position == "latest") {
      startPos = StartupOptions.latest()
    }

    val prop = new Properties()
    prop.setProperty("decimal.handling.mode", "string")
    MySqlSource.builder[String]
      .hostname(params.host.split(":")(0))
      .port(params.host.split(":")(1).toInt)
      .username(params.username)
      .password(params.pwd)
      .databaseList(params.dbList)
      .tableList(params.tbList)
      .startupOptions(startPos)
      .serverId(params.serverId)
      .debeziumProperties(prop)
      .deserializer(new JsonDebeziumDeserializationSchema).build
  }

  def createKafkaSink(params: Config) = {
    val clientId = "mysql-cdc-" + java.util.UUID.randomUUID

    val kafkaProducerProperties = Map(
//      "security.protocol"-> "SASL_SSL",
//      "sasl.mechanism"-> "AWS_MSK_IAM",
//      "sasl.jaas.config"-> "software.amazon.msk.auth.iam.IAMLoginModule required;",
//      "sasl.client.callback.handler.class"-> "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        "transaction.timeout.ms"-> "900000",
        "client.id.prefix"-> "mysql-cdc"
    )

    KafkaSink.builder[String]()
      .setBootstrapServers(params.brokerList)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic(params.sinkTopic)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setKafkaProducerConfig(kafkaProducerProperties)
      .setTransactionalIdPrefix("mysql-cdc")
      .build()
  }

  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
  }

  def main(args: Array[String]): Unit = {
    println(args.mkString)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = Config.parseConfig(MySQLCDC, args)

    env.enableCheckpointing(params.checkpointInterval.toInt * 1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStateBackend(new EmbeddedRocksDBStateBackend)
    env.getCheckpointConfig.setCheckpointStorage(params.checkpointDir)

    env.fromSource(createCDCSource(params), WatermarkStrategy.noWatermarks(), "mysql cdc source")
      .sinkTo(createKafkaSink(params)).name("cdc sink msk")
      .setParallelism(params.parallel.toInt)

    env.execute("MySQL Binlog CDC")
  }
}

//sudo aws s3 cp s3://airflow-us-east-1-551831295244/jar/flink-connector-kafka-1.15.2.jar /usr/lib/flink/lib/
//sudo aws s3 cp s3://airflow-us-east-1-551831295244/jar/flink-sql-connector-mysql-cdc-2.2.1.jar /usr/lib/flink/lib/
//sudo aws s3 cp s3://airflow-us-east-1-551831295244/jar/original-emr-flink-cdc-1.0-SNAPSHOT.jar /home/hadoop/
//sudo sed -i -e '$a\classloader.check-leaked-classloader: false' /etc/flink/conf/flink-conf.yaml

//sudo flink run -m yarn-cluster \
//  -yjm 1024 -ytm 1024 -d \
//  -ys 4 -p 8 \
//  -c com.aws.analytics.MySQLCDC /home/hadoop/emr-flink-cdc-1.0-SNAPSHOT.jar \
//  -b b-2.emrworkshopmsk.jlp7a8.c14.kafka.us-east-1.amazonaws.com:9092,b-1.emrworkshopmsk.jlp7a8.c14.kafka.us-east-1.amazonaws.com:9092 \
//  -t mysql-cdc-flink \
//  -c s3://dalei-demo/checkpoint/ \
//  -l 30 -h emr-workshop-mysql8.chl9yxs6uftz.us-east-1.rds.amazonaws.com:3306 -u admin -P ***** -d test_db -T test_db.* \
//  -p 4 \
//  -e 5400-5408

