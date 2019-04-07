package com.lucas.boot.job


import java.text.SimpleDateFormat

import com.lucas.boot.service.StatisticService
import com.lucas.boot.spark.vo.SceneQueryVO

/**
  * Job
  *
  * @author yupeng10@baidu.com
  * @since 1.0
  */
object Job {
  def main(args: Array[String]): Unit = {
    val startTime = "2019-01-01 00:00:00"
    val endTime = "2019-02-01 00:00:00"
    pvuvperson(startTime, endTime)
    avgstaytimescene(startTime, endTime)
    agescene(startTime, endTime)
    genderscene(startTime, endTime)
  }


  private def avgstaytimescene(startTime: String, endTime: String) = {
    val queryVO: SceneQueryVO = new SceneQueryVO()
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    queryVO.setStartTime(sdf.parse(startTime).getTime)
    queryVO.setStartTimeStr(startTime)
    queryVO.setEndTimeStr(endTime)
    queryVO.setEndTime(sdf.parse(endTime).getTime)
    queryVO.setCollectionId(29L)
    queryVO.setSceneId(32L)
    queryVO.setInterval(Interval.Month.toString)
    StatisticService.statisticavgStaytime(queryVO)
  }


  private def agescene(startTime: String, endTime: String) = {
    val queryVO: SceneQueryVO = new SceneQueryVO()
    queryVO.setStartTimeStr(startTime)
    queryVO.setInterval(Interval.Month.toString)
    StatisticService.statisticage(queryVO)
  }

  private def genderscene(startTime: String, endTime: String) = {
    val queryVO: SceneQueryVO = new SceneQueryVO()
    queryVO.setStartTimeStr(startTime)
    queryVO.setInterval(Interval.Month.toString)
    StatisticService.statisticgender(queryVO)
  }

  private def pvuvperson(startTime: String, endTime: String) = {
    val queryVO: SceneQueryVO = new SceneQueryVO()
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    queryVO.setStartTime(sdf.parse(startTime).getTime)
    queryVO.setStartTimeStr(startTime)
    queryVO.setEndTimeStr(endTime)
    queryVO.setEndTime(sdf.parse(endTime).getTime)
    queryVO.setCollectionId(29L)
    queryVO.setSceneId(32L)
    queryVO.setInterval(Interval.Month.toString)
    StatisticService.statisticScenePVUVPerson(queryVO)
  }
}
