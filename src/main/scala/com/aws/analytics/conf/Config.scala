package com.aws.analytics.conf

case class Config(
                   brokerList: String = "",
                   sinkTopic: String = "",
                   checkpointDir: String ="",
                   checkpointInterval:String ="60",
                   host:String="",
                   username:String="",
                   pwd:String="",
                   dbList:String="",
                   tbList:String="",
                   parallel:String="1",
                   position:String="initial",
                   sourceTopic:String="",
                   groupId:String="",
                   kdsName: String = "",
                   kdsRegion: String="",
                   serverId:String="",
                   hudiBasePath:String=""
                 )

object Config {

  def parseConfig(obj: Object,args: Array[String]): Config = {
    val programName = obj.getClass.getSimpleName.replaceAll("\\$","")
    val parser = new scopt.OptionParser[Config](programName) {
      head(programName, "1.0")
      opt[String]('b', "brokerList").required().action((x, config) => config.copy(brokerList = x)).text("kafka broker list,sep comma")
      opt[String]('c', "checkpointDir").required().action((x, config) => config.copy(checkpointDir = x)).text("checkpoint dir")
      opt[String]('l', "checkpointInterval").optional().action((x, config) => config.copy(checkpointInterval = x)).text("checkpoint interval: default 60 seconds")

      programName match {
        case "MySQLCDC" =>
          opt[String]('p', "parallel").optional().action((x, config) => config.copy(parallel = x)).text("cdc source parallel")
          opt[String]('t', "sinkTopic").required().action((x, config) => config.copy(sinkTopic = x)).text("kafka topic")
          opt[String]('h', "host").required().action((x, config) => config.copy(host = x)).text("mysql hostname, eg. localhost:3306")
          opt[String]('u', "username").required().action((x, config) => config.copy(username = x)).text("mysql username")
          opt[String]('P', "pwd").required().action((x, config) => config.copy(pwd = x)).text("mysql password")
          opt[String]('d', "dbList").required().action((x, config) => config.copy(dbList = x)).text("cdc database list: db1,db2,..,dbn")
          opt[String]('T', "tbList").required().action((x, config) => config.copy(tbList = x)).text("cdc table list: db1.*,db2.*,db3.tb*...,dbn.*")
          opt[String]('s', "position").optional().action((x, config) => config.copy(position = x)).text("cdc start position: initial or latest,default: initial")
          opt[String]('e', "serverId").optional().action((x, config) => config.copy(serverId = x)).text("cdc server id")

        case "Kafka2Hudi" =>
          opt[String]('s', "sourceTopic").required().action((x, config) => config.copy(sourceTopic = x)).text("kafka cdc source topic")
          opt[String]('p', "groupId").required().action((x, config) => config.copy(groupId = x)).text("consumer group id")
          opt[String]('g', "hudiBasePath").required().action((x, config) => config.copy(hudiBasePath = x)).text("hudi base path")

        case _ =>
      }


    }
    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None => {
        //        println("cannot parse args")
        System.exit(-1)
        null
      }
    }

  }

}
