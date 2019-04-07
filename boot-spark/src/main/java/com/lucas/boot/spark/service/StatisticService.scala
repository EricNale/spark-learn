package com.lucas.boot.service

import java.util

import com.lucas.boot.job.{StatisticEnum, StatisticTypeEnum}
import com.lucas.boot.spark.utils.SparkUtils
import com.lucas.boot.spark.vo.SceneQueryVO
import com.lucas.boot.utils.StatisticUtils
import com.mongodb.spark.MongoSpark
import org.bson.Document

/**
  * StatisticService
  *
  * @author yupeng10@baidu.com
  * @since 1.0
  */
object StatisticService {

  def statisticScenePVUVPerson(queryVO: SceneQueryVO): Unit = {

    // build conditionSql
    val pair = StatisticUtils.buildRdd(queryVO, StatisticEnum.SCENE_PV_UV_PERSON)
    queryVO.setStatisticType(StatisticTypeEnum.pv_uv_person.toString)
    val queryMap = SparkUtils.beanToMap(queryVO)

    // build map
    val pvRdd = pair._1.map(x => ( {
      val bo = scala.collection.mutable.Map[String, Object]()
      bo.put("sceneId", x.get("sceneId"))
      bo.put("unionUid", x.get("unionUid"))
      bo
    }, x))

    // build reduce=> pv uv
    val saveRdd = pvRdd.groupByKey().map(x => {
      val map = new java.util.HashMap[String, Object]
      map.putAll(queryMap)
      val document = x._2.iterator.next()
      val bo = new Document()
      map.put("sceneId", document.get("sceneId"))
      map.put("unionUid", document.get("unionUid"))
      map.put("faceInfo", document.get("faceInfo"))
      val result = new Document()
      result.put("uv", 1)
      result.put("pv", x._2.size)
      bo.put("values", result)
      bo.put("_id", map)
      bo
    })
    MongoSpark.save(saveRdd)
  }


  def statisticavgStaytime(queryVO: SceneQueryVO): Unit = {
    val pair = StatisticUtils.buildRdd(queryVO, StatisticEnum.SCENE_AVG_STAYTIME_PERSON)
    queryVO.setStatisticType(StatisticTypeEnum.avg_stay_time_person.toString)
    val pvRdd = pair._1.map(x => ( {
      val bo = new util.HashMap[String, Object]()
      bo.put("sceneId", x.get("sceneId"))
      bo.put("unionUid", x.get("unionUid"))
      bo
    }, x))
    val queryMap = SparkUtils.beanToMap(queryVO)
    val saveRdd = pvRdd.groupByKey().map(x => {
      val d = x._2.iterator
      val sorted = d.toStream.sortWith((a, b) => {
        a.get("snapshotTimel").asInstanceOf[Long] < b.get("snapshotTimel").asInstanceOf[Long]
      })
      var inIndex = 0
      var inflag = 0
      var i = 0
      var statisticValue: Double = 0
      var statisticCount = 0
      sorted.foreach(x => {
        if (x.get("cameraPositionType") == "IN") {
          inIndex = i
          inflag = 1
        } else if (x.get("cameraPositionType") == "OUT" && inflag != 0) {
          inflag = 0
          statisticValue += x.get("snapshotTimel").asInstanceOf[Long] - sorted(inIndex).get("snapshotTimel").asInstanceOf[Long]
          statisticCount += 1
        }
        i += 1
      })
      val document = sorted.head
      val stayTime = Math.ceil(statisticValue / (1000 * 60));
      statisticValue = stayTime
      val bo = new Document()
      val idMap = new util.HashMap[String, Object]()
      idMap.putAll(queryMap)
      idMap.putAll(x._1)
      idMap.put("faceInfo", document.get("faceInfo"))
      bo.put("_id", idMap)

      val values = new util.HashMap[String, Object]()
      values.put("statisticValue", statisticValue.asInstanceOf[Object])
      values.put("statisticCount", statisticCount.asInstanceOf[Object])
      bo.put("values", values)
      bo
    })
    MongoSpark.save(saveRdd)
  }

  def statisticgender(queryVO: SceneQueryVO): Unit = {
    val pair = StatisticUtils.buildRdd(queryVO, StatisticEnum.SCENE_PV_UV_STATISTIC)
    queryVO.setStatisticType(StatisticTypeEnum.gender.toString)
    val pvRdd = pair._1.map(x => ( {
      val bo = new Document()
      val id = x.get("_id").asInstanceOf[Document]
      bo.put("sceneId", id.get("sceneId"))
      bo.put("statisticKey", id.get("faceInfo").asInstanceOf[Document].get("gender"))
      bo
    }, {
      x.get("values").asInstanceOf[Document]
    }))
    val queryMap = SparkUtils.beanToMap(queryVO)
    val result = pvRdd.reduceByKey((x, y) => StatisticProcessor.pvuvProcessor(x, y))
    val saveRdd = result.map(x => {
      val idMap = new util.HashMap[String, Object]()
      idMap.putAll(queryMap)
      idMap.putAll(x._1)
      val bo = new Document()
      bo.put("_id", idMap)
      bo.put("values", x._2)
      bo
    })
    MongoSpark.save(saveRdd)
  }

  def statisticage(queryVO: SceneQueryVO): Unit = {
    val pair = StatisticUtils.buildRdd(queryVO, StatisticEnum.SCENE_PV_UV_STATISTIC)
    queryVO.setStatisticType(StatisticTypeEnum.age.toString)
    val queryMap = SparkUtils.beanToMap(queryVO)
    val pvRdd = pair._1.map(x => ( {
      val bo = new Document()
      val id = x.get("_id").asInstanceOf[Document]
      bo.put("sceneId", id.get("sceneId"))
      bo.put("statisticKey", id.get("faceInfo").asInstanceOf[Document].get("age"))
      bo
    }, {
      x.get("values").asInstanceOf[Document]
    }))
    val result = pvRdd.reduceByKey((x, y) => StatisticProcessor.pvuvProcessor(x, y))
    val saveRdd = result.map(x => {
      val idMap = new util.HashMap[String, Object]()
      idMap.putAll(queryMap)
      idMap.putAll(x._1)
      val bo = new Document()
      bo.put("_id", idMap)
      bo.put("values", x._2)
      bo
    })
    MongoSpark.save(saveRdd)
  }


}
