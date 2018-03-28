package com.github.takezoe.predictionio.toolbox

import org.apache.commons.io.FileUtils
import org.apache.predictionio.data.storage._
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import scala.concurrent.ExecutionContext.Implicits.global

case class PIOToolbox(pioHome: String) {

  private val file = new java.io.File(s"$pioHome/conf/pio-env.sh")

  val config: Map[String, String] = Common.processEnvVars(pioHome,
    FileUtils.readFileToString(file).split("\n").map { line =>
      if(line.trim.startsWith("#") || line.trim.length == 0){
        None
      } else {
        val Array(key, value) = line.split("=").map(_.trim)
        Some((key, value))
      }
    }.flatten.toSeq)

  EnvironmentFactory.environmentService = Some(new EnvironmentService(){
    override def envKeys(): Iterable[String] = config.keys
    override def getByKey(key: String): String = config.get(key).orNull
    override def filter(filterExpression: ((String, String)) => Boolean): Map[String, String] = config.filter(filterExpression)
  })

  def find(
    appName: String,
    channelName: Option[String] = None,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    entityType: Option[String] = None,
    entityId: Option[String] = None,
    eventNames: Option[Seq[String]] = None,
    targetEntityType: Option[Option[String]] = None,
    targetEntityId: Option[Option[String]] = None
  )(sc: SparkContext): RDD[Event] = {
    PEventStore.find(
      appName,
      channelName,
      startTime,
      untilTime,
      entityType,
      entityId,
      eventNames,
      targetEntityType,
      targetEntityId
    )(sc)
  }

  def write(
    events: RDD[Event],
    appName: String,
    channelName: Option[String] = None
  )(sc: SparkContext): Unit = {
    val (appId, channelId) = Common.appNameToId(appName, channelName)
    Storage.getPEvents().write(events, appId, channelId)(sc)
  }

  def insert(
    event: Event,
    appName: String,
    channelName: Option[String] = None
  ): Unit = {
    val (appId, channelId) = Common.appNameToId(appName, channelName)
    Storage.getLEvents().futureInsert(event, appId, channelId)
  }

  def insertBatch(
    events: Seq[Event],
    appName: String,
    channelName: Option[String] = None
  ): Unit = {
    val (appId, channelId) = Common.appNameToId(appName, channelName)
    Storage.getLEvents().futureInsertBatch(events, appId, channelId)
  }

}
