package com.lucas.boot.dto

/**
  * ScenePvuv
  *
  * @author yupeng10@baidu.com
  * @since 1.0
  */
case class ScenePvuv(_id: SceneId, values: Pvuv)

case class SceneId(sceneId: Long, interval: String, startTime: Long, endTime: Long, unionUid: String, faceInfo: FaceInfo)

case class FaceInfo(age: Double, gender: String)

case class Pvuv(uv: Int, pv: Int)