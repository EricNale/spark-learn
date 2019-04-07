package com.lucas.boot.service

import com.lucas.boot.job.StatisticEnum
import com.lucas.boot.spark.utils.SparkUtils
import com.mongodb.spark.MongoSpark
import org.bson.Document

/**
  * ImportData
  *
  * @author yupeng10@baidu.com
  * @since 1.0
  */
object ImportData {
  def main(args: Array[String]): Unit = {
    //    val sql = "{$match:{\"snapshotTime\":{$gt:ISODate(\"2019-01-01T00:00:00.000CST\"), $lt:ISODate(\"2019-03-01T00:00:00.000CST\")}}}"
    val spark = SparkUtils.buildSession(StatisticEnum.SYNC.getInput, StatisticEnum.SYNC.getOutput)
    val rdd = MongoSpark.load(spark.sparkContext)
    MongoSpark.save(rdd)

    //    val rdd = MongoSpark.load(spark.sparkContext).withPipeline(Seq(Document.parse(sql)))
    //    MongoSpark.save(rdd)
    //    val filterRdd = rdd.filter(x => {
    //      val bo = new BasicDBObject()
    //      val logExtInfo = x.get("logExtInfo").asInstanceOf[Document]
    //      val deviceInfo = logExtInfo.get("deviceInfo").asInstanceOf[Document]
    //      deviceInfo != null
    //    })
    //    val flowRdd = filterRdd.map(x => ( {
    //      val bo = new BasicDBObject()
    //      val logExtInfo = x.get("logExtInfo").asInstanceOf[Document]
    //      val deviceInfo = logExtInfo.get("deviceInfo").asInstanceOf[Document]
    //      bo.put("followId", deviceInfo.get("followId"))
    //      bo
    //    }, x))
    //
    //    val resultRdd = flowRdd.groupByKey().map(x => {
    //      var currentTime = 0L
    //      var value = 1
    //      for (doc <- x._2) {
    //        val current = doc.get("snapshotTimel").asInstanceOf[Long]
    //        if (currentTime == 0) {
    //          currentTime = current
    //        } else {
    //          if (current - currentTime > 5000) {
    //            value += 1
    //            currentTime = current
    //          }
    //        }
    //      }
    //      val bo = new BasicDBObject()
    //      bo.put("_id", x._1.get("followId"))
    //      bo.put("values", new Document("count", value))
    //      bo
    //    })
    //    resultRdd.saveAsTextFile("result")
  }
}
