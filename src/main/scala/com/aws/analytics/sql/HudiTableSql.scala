package com.aws.analytics.sql

object HudiTableSql {
  def createTB1(s3Path:String, sinkTB:String)={
      s"""CREATE TABLE $sinkTB(
         |id string primary key,
         |name string,
         |device_model string,
         |email string,
         |phone string,
         |create_time string,
         |modify_time string
         |)
         |WITH (
         |  'connector' = 'hudi',
         |  'path' = '$s3Path/$sinkTB/',
         |  'table.type' = 'COPY_ON_WRITE',
         |  'write.precombine.field' = 'modify_time',
         |  'write.operation' = 'upsert',
         |  'write.payload.class' = 'org.apache.hudi.common.model.DefaultHoodieRecordPayload',
         |  'write.keygenerator.class' = 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
         |  'write.recordkey.field' = 'id',
         |  'hoodie.datasource.write.recordkey.field' = 'id',
         |  'hive_sync.enable' = 'true',
         |  'hive_sync.mode' = 'hms',
         |  'hive_sync.use_jdbc' = 'false',
         |  'hive_sync.database' = 'dev',
         |  'hive_sync.table' = '$sinkTB',
         |  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.NonPartitionedExtractor'
         |  )
      """.stripMargin
  }

  def createTB2(s3Path:String,sinkTB:String)={
    s"""CREATE TABLE  $sinkTB(
       |pid string primary key,
       |pname string,
       |pprice string,
       |create_time string,
       |modify_time string
       |)
       |WITH (
       | 'connector' = 'hudi',
       |  'path' = '$s3Path/$sinkTB/',
       |  'table.type' = 'COPY_ON_WRITE',
       |  'write.precombine.field' = 'modify_time',
       |  'write.operation' = 'upsert',
       |  'write.payload.class' = 'org.apache.hudi.common.model.DefaultHoodieRecordPayload',
       |  'write.keygenerator.class' = 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
       |  'write.recordkey.field' = 'pid',
       |  'hive_sync.enable' = 'true',
       |  'hive_sync.mode' = 'hms',
       |  'hive_sync.use_jdbc' = 'false',
       |  'hive_sync.database' = 'dev',
       |  'hive_sync.table' = '$sinkTB',
       |  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.NonPartitionedExtractor'
       |  )
      """.stripMargin
  }

  def createTB3(s3Path:String, sinkTB:String)={
    s"""CREATE TABLE  $sinkTB(
       |id string primary key,
       |oid string,
       |uid string,
       |pid string,
       |onum string,
       |create_time string,
       |modify_time string,
       |logday VARCHAR(255),
       |hh VARCHAR(255)
       |)PARTITIONED BY (`logday`,`hh`)
       |WITH (
       | 'connector' = 'hudi',
       |  'path' = '$s3Path/$sinkTB/',
       |  'table.type' = 'COPY_ON_WRITE',
       |  'write.precombine.field' = 'modify_time',
       |  'write.operation' = 'upsert',
       |  'write.payload.class' = 'org.apache.hudi.common.model.DefaultHoodieRecordPayload',
       |  'write.recordkey.field' = 'id',
       |  'write.partitionpath.field' = 'logday,hh',
       |  'hive_sync.enable' = 'true',
       |  'hive_sync.mode' = 'hms',
       |  'hive_sync.use_jdbc' = 'false',
       |  'hive_sync.database' = 'dev',
       |  'hive_sync.table' = '$sinkTB',
       |  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
       |  )
      """.stripMargin
  }

  def insertTB1SQL(sinkTB:String, prefix:String)={
    s"""
       |insert into $sinkTB select * from $prefix$sinkTB
       |""".stripMargin
  }

  def insertTB3SQL(sinkTB:String, prefix:String)={
    s"""
       |insert into $sinkTB select *, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') as logday, DATE_FORMAT(CURRENT_TIMESTAMP, 'hh') as hh  from $prefix$sinkTB
       |""".stripMargin
  }
}
