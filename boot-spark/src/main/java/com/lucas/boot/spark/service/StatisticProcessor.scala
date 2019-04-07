package com.lucas.boot.service

import org.bson.Document

/**
  * StatisticProcessor
  *
  * @author yupeng10@baidu.com
  * @since 1.0
  */
object StatisticProcessor {

  def pvuvProcessor(X: Document, Y: Document): Document = {
    val bo = new Document()
    bo.put("pv", X.get("pv").asInstanceOf[Int] + Y.get("pv").asInstanceOf[Int])
    bo.put("uv", X.get("uv").asInstanceOf[Int] + Y.get("uv").asInstanceOf[Int])
    bo
  }


}
