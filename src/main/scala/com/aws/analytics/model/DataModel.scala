package com.aws.analytics.model

object DataModel {

  case class TableUser(id:String, name:String, device_model:String, email:String, phone:String, create_time:String, modify_time:String)
  case class TableProduct(pid:String, pname:String, pprice:String, create_time:String,modify_time:String)
  case class TableUser_Order(id:String, oid:String, uid:String, pid:String, onum:String, create_time:String,modify_time:String)

}
