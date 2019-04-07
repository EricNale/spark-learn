package com.lucas.boot.utils

import com.lucas.boot.job.{StatisticEnum, VmTemplate}
import com.lucas.boot.spark.utils.SparkUtils
import com.lucas.boot.spark.vo.ConditionQueryVO
import com.mongodb.spark.MongoSpark
import org.apache.velocity.VelocityContext
import org.bson.Document

/**
  * StatisticUtils
  *
  * @author yupeng10@baidu.com
  * @since 1.0
  */
object StatisticUtils {
  /**
    * condition build
    *
    * @param conditionQuery
    * @param statisticEnum
    * @return
    */
  def buildRdd(conditionQuery: ConditionQueryVO, statisticEnum: StatisticEnum) = {
    val velocityContext = new VelocityContext()
    val map = SparkUtils.beanToMap(conditionQuery)
    velocityContext.put("param", map)
    val vm = VmTemplate.getTemplate(velocityContext, statisticEnum.getMatch)
    val sparkSession = SparkUtils.buildSession(statisticEnum.getInput, statisticEnum.getOutput)
    sparkSession.conf.set("spark.mongodb.input.uri", statisticEnum.getInput)
    sparkSession.conf.set("spark.mongodb.output.uri", statisticEnum.getOutput)
    val rdd = MongoSpark.load(sparkSession.sparkContext).withPipeline(Seq(Document.parse(vm.toJSONString)))
    (rdd, sparkSession)
  }

}
