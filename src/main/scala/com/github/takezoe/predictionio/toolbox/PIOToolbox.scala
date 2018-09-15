package com.github.takezoe.predictionio.toolbox

import java.io.File

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

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global

case class PIOToolbox(pioHome: String) {

  private val templateDir = new File(s"$pioHome/templates")
  private val file = new java.io.File(s"$pioHome/conf/pio-env.sh")
  private implicit val formats = Serialization.formats(NoTypeHints)
  private val managedApps = new ListBuffer[PIOApp]()

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

  def apps(): String = {
    val apps = storage.Storage.getMetaDataApps()
    val keys = storage.Storage.getMetaDataAccessKeys()

    "%html\n" +
      "<table border=\"1\">" +
      "<tr><th>ID</th><th>Name</th><th>Status</th><th>Access Key</th><th>Directory</th></tr>" +
      (apps.getAll().map { app =>
        val appKeys = keys.getByAppid(app.id)
        (if(appKeys.isEmpty) Seq(AccessKey("None", app.id, Seq("None"))) else appKeys).map { key =>
          "<tr>" +
            "<td>" + app.id + "</td>" +
            "<td>" + app.name + "</td>" +
            "<td>" + {
              managedApps.find(_.appName == app.name) match {
                case Some(app) => if(app.runningInfo == null) "Stop" else s"Running: ${app.runningInfo.port}"
                case None => "Unknown"
              }
            } + "</td>" +
            "<td>" + key.key + "</td>" +
            "<td>" + {
              val dir = new File(s"$templateDir/${app.name}")
              if(dir.exists()) dir.getAbsolutePath else "None"
            } + "</td>" +
            "</tr>"
        }.mkString
      }.mkString) +
      "</table>"
  }

  def deleteApp(appName: String): Unit = {
    val apps = storage.Storage.getMetaDataApps()
    apps.getByName(appName).foreach { x =>
      apps.delete(x.id)
    }

    val dir = new File(templateDir, appName)
    if(dir.exists()){
      FileUtils.deleteQuietly(dir)
    }
  }

  def createApp(appName: String, templateUrl: String): PIOApp = {
    val apps = storage.Storage.getMetaDataApps()
    val keys = storage.Storage.getMetaDataAccessKeys()

    val app = apps.getByName(appName) match {
      case Some(_) =>
        val dir = new File(templateDir, appName)
        if(!dir.exists()){
          throw new IllegalStateException(s"[${appName}] Application is registered but template does not exist! " +
            s"Try to run `toolbox.deleteApp(appName)` before creating the application.")
        }

        // Check template can be used out-of-the-box
        val file = new File(dir, "engine.json")
        if(!file.exists()){
          throw new IllegalStateException(s"[${appName}] engine.json does not exist!")
        }

        val json = parse(FileUtils.readFileToString(file))
        val algorithms = Serialization.write((json \ "algorithms"))

        PIOApp(this, appName, templateUrl, dir, algorithms)

      case None =>
        val appId = apps.insert(App(0, appName, None))
        keys.insert(AccessKey("", appId.get, Nil))

        val dir = new File(templateDir, appName)

        // Clone template
        println(s"[${appName}] Cloning template...")
        Git.cloneRepository().setURI(templateUrl).setDirectory(dir).call()

        // Check template can be used out-of-the-box
        val file = new File(dir, "engine.json")
        if(!file.exists()){
          throw new IllegalStateException(s"[${appName}] engine.json does not exist!")
        }

        // Update appName
        val json = parse(FileUtils.readFileToString(file))
          .merge(JObject(JField("datasource", JObject(JField("params", JObject(JField("appName", JString(appName))))))))
        val jsonString = Serialization.write(json)
        FileUtils.write(file, jsonString)

        val algorithms = Serialization.write((json \ "algorithms"))

        PIOApp(this, appName, templateUrl, dir, algorithms)
    }

    managedApps += app
    app
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
