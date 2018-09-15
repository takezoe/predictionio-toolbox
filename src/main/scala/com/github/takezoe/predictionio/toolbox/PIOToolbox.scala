package com.github.takezoe.predictionio.toolbox

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.predictionio.data.storage
import org.apache.predictionio.data.storage._
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.eclipse.jgit.api.Git
import org.joda.time.DateTime
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization

import scala.concurrent.ExecutionContext.Implicits.global

case class PIOToolbox(pioHome: String) {

  private val file = new java.io.File(s"$pioHome/conf/pio-env.sh")
  private implicit val formats = Serialization.formats(NoTypeHints)

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

  def apps(): Seq[Map[String, Any]] = {
    val apps = storage.Storage.getMetaDataApps()
    val keys = storage.Storage.getMetaDataAccessKeys()

    apps.getAll().map { app =>
      Map("id" -> app.id, "name" -> app.name, "description" -> app.description, "accessKey" -> keys.getByAppid(app.id))
    }
  }

  def createApp(appName: String, templateUrl: String): PIOApp = {
    val apps = storage.Storage.getMetaDataApps()
    apps.getByName(appName) match {
      case Some(_) =>
        println(s"Application: ${appName} already exists.")
      case None =>
        println(s"Create application: ${appName}")
        apps.insert(App(0, appName, None))
    }

    // Clone template
    val dir = Files.createTempDirectory(appName).toFile
    Git.cloneRepository().setURI(templateUrl).setDirectory(dir).call()

    // Check template can be used out-of-the-box
    val file = new File(dir, "engine.json")
    if(!file.exists()){
      throw new IllegalStateException("This template isn't able to be used out-of-the-box!")
    }

    // Update appName
    val json = parse(FileUtils.readFileToString(file))
      .merge(JObject(JField("datasource", JObject(JField("params", JObject(JField("appName", JString(appName))))))))
    val jsonString = Serialization.write(json)
    FileUtils.write(file, jsonString)

    val algorithms = Serialization.write((json \ "algorithms"))

    PIOApp(this, appName, templateUrl, dir, algorithms)
  }

  def findEventRDD(
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

  def writeEventRDD(
    events: RDD[Event],
    appName: String,
    channelName: Option[String] = None
  )(sc: SparkContext): Unit = {
    val (appId, channelId) = Common.appNameToId(appName, channelName)
    Storage.getPEvents().write(events, appId, channelId)(sc)
  }

  def insertEvent(
    event: Event,
    appName: String,
    channelName: Option[String] = None
  ): Unit = {
    val (appId, channelId) = Common.appNameToId(appName, channelName)
    Storage.getLEvents().futureInsert(event, appId, channelId)
  }

  def insertEventBatch(
    events: Seq[Event],
    appName: String,
    channelName: Option[String] = None
  ): Unit = {
    val (appId, channelId) = Common.appNameToId(appName, channelName)
    Storage.getLEvents().futureInsertBatch(events, appId, channelId)
  }

}
