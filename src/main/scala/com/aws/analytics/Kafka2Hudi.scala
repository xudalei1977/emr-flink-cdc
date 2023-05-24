package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.model.DataModel
import com.aws.analytics.sql.HudiTableSql
import com.google.gson.GsonBuilder
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.logging.log4j.LogManager
import com.google.gson.JsonObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.kafka.common.serialization.StringDeserializer



object Kafka2Hudi{

  private val log = LogManager.getLogger(Kafka2Hudi.getClass)
  private val gson = new GsonBuilder().create

  def createKafkaSource(env: StreamExecutionEnvironment, param: Config): DataStream[String] = {

//      val properties = new Properties()
//      properties.setProperty("bootstrap.servers", parmas.brokerList)
//      properties.setProperty("group.id", parmas.groupId)
//
//      val myConsumer = new KafkaSource[String](
//        parmas.sourceTopic,
//        new SimpleStringSchema(),
//        properties)

    val kafkaSource = KafkaSource.builder[String]
        .setBootstrapServers(param.brokerList)
        .setBounded(OffsetsInitializer.latest)
        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))
        .setTopics(param.sourceTopic)
        .setGroupId(param.groupId)
        .build

    env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
  }

  def main(args: Array[String]) {
    val params = Config.parseConfig(Kafka2Hudi, args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(params.checkpointInterval.toInt * 1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStateBackend(new EmbeddedRocksDBStateBackend)
    env.getCheckpointConfig.setCheckpointStorage(params.checkpointDir)

    // create environments of both APIs
    val tableEnv = StreamTableEnvironment.create(env)

    val conf = new Configuration()
    conf.setString("pipeline.name", "kafka-mysql-cdc-hudi");
    tableEnv.getConfig.addConfiguration(conf)

    // create kafka source
    val source = createKafkaSource(env, params)

    val sinkTableUser = "users"
    val sinkTableProduct = "product"
    val sinkTableUserOrder = "user_order"
    val s3Path = params.hudiBasePath

    val ot1 = OutputTag[DataModel.TableUser](sinkTableUser)
    val ot2 = OutputTag[DataModel.TableProduct](sinkTableProduct)
    val ot3 = OutputTag[DataModel.TableUser_Order](sinkTableUserOrder)

    val outputStream = source.process(new ProcessFunction[String, String] {
      override def processElement(
                                     value: String,
                                     ctx: ProcessFunction[String, String]#Context,
                                     out: Collector[String]): Unit = {
        try {
          val jsonObj = gson.fromJson(value, classOf[JsonObject])
          val db = jsonObj.get("source").getAsJsonObject.get("db").getAsString
          val table = jsonObj.get("source").getAsJsonObject.get("table").getAsString
          val afterData = jsonObj.get("after").getAsJsonObject.toString
          db match {
            case "test_db" =>
              table match {
                case "user" =>  ctx.output(ot1, gson.fromJson(afterData,classOf[DataModel.TableUser]))
                case "product" =>  ctx.output(ot2, gson.fromJson(afterData,classOf[DataModel.TableProduct]))
                case "user_order" =>  ctx.output(ot3,gson.fromJson(afterData,classOf[DataModel.TableUser_Order]))
              }
            case _ => out.collect(value)
          }
        }catch {
          case e: Exception => {
            log.error(e.getMessage)
          }
        }
      }
    })

    val prefix = "source_"
    val ot1Table = tableEnv.fromDataStream(outputStream.getSideOutput(ot1))
    tableEnv.createTemporaryView(prefix + sinkTableUser, ot1Table)

    val ot2Table = tableEnv.fromDataStream(outputStream.getSideOutput(ot2))
    tableEnv.createTemporaryView(prefix + sinkTableProduct, ot2Table)

    val ot3Table = tableEnv.fromDataStream(outputStream.getSideOutput(ot3))
    tableEnv.createTemporaryView(prefix + sinkTableUserOrder, ot3Table)

    tableEnv.executeSql(HudiTableSql.createTB1(s3Path, sinkTableUser))
    tableEnv.executeSql(HudiTableSql.createTB2(s3Path, sinkTableProduct))
    tableEnv.executeSql(HudiTableSql.createTB3(s3Path, sinkTableUserOrder))

    val stat = tableEnv.createStatementSet()
    stat.addInsertSql(HudiTableSql.insertTB1SQL(sinkTableUser, prefix))
    stat.addInsertSql(HudiTableSql.insertTB1SQL(sinkTableProduct, prefix))
    stat.addInsertSql(HudiTableSql.insertTB3SQL(sinkTableUserOrder, prefix))
    stat.execute()
  }
}



//sudo flink run -m yarn-cluster \
//-yjm 1024 -ytm 1024 -d -ys 4 -p 8 \
//-c com.aws.analytics.Kafka2Hudi /home/hadoop/original-emr-flink-cdc-1.0-SNAPSHOT.jar \
//-b b-1.emrworkshopmsk1.q9wrkd.c14.kafka.us-east-1.amazonaws.com:9092,b-2.emrworkshopmsk1.q9wrkd.c14.kafka.us-east-1.amazonaws.com:9092,b-3.emrworkshopmsk1.q9wrkd.c14.kafka.us-east-1.amazonaws.com:9092 \
//-s mysql-flink-cdc -p msk-consumer-group-01 -c s3://airflow-us-east-1-551831295244/checkpoint -l 30 \
//-g s3://airflow-us-east-1-551831295244/dev