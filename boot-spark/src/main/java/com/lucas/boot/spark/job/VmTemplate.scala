package com.lucas.boot.job

import java.io.StringWriter
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.velocity.VelocityContext
import org.apache.velocity.app.Velocity

/**
  * 加载模板
  *
  * @author yupeng10@baidu.com
  * @since 1.0
  */
object VmTemplate {

  val template: Unit = {
    val p = new Properties
    p.put("file.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader")
    try
      Velocity.init(p)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def getTemplate(context: VelocityContext, templateFile: String): JSONObject = {
    val writer = new StringWriter()
    val template = Velocity.getTemplate(templateFile)
    template.merge(context, writer)
    JSON.parseObject(writer.toString)
  }
}
