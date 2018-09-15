package com.github.takezoe.predictionio.toolbox

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.predictionio.data.storage.{Event, Storage}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.JsonMethods.parse

import scala.collection.JavaConverters._

case class Algorithm(name: String, params: Map[String, Any])

case class PIOApp(
  toolbox: PIOToolbox,
  appName: String,
  url: String,
  dir: File,
  algorithms: String
){
  private implicit val formats = Serialization.formats(NoTypeHints)

  def build(): Int = {
    println("Compiling application...")
    executeCommand(Seq(s"${toolbox.pioHome}/bin/pio", "build"))
  }

  def open(): Unit = {
    executeCommand(Seq("open", dir.getAbsolutePath))
  }

  def train(algorithms: String = ""): Int = {
    if(algorithms.nonEmpty){
      // Update configuration
      val configJson = JObject(JField("algorithms", parse(algorithms)))

      val file = new File(dir, "engine.json")
      val json = parse(FileUtils.readFileToString(file)).merge(configJson)

      val jsonString = Serialization.write(json)
      FileUtils.write(file, jsonString)
    }

    println("Training model...")
    executeCommand(Seq(s"${toolbox.pioHome}/bin/pio", "train"))
  }

  // TODO How to shutdown deployed API?
  def deploy(): Int = {
    println("Deploying service...")
    executeCommand(Seq(s"${toolbox.pioHome}/bin/pio", "deploy"))
  }

  private def executeCommand(command: Seq[String]): Int = {
    val builder = new ProcessBuilder(command: _*).directory(dir)

    // Remove SPARK keys
    val keys = builder.environment().asScala.collect { case (key, value) if key.startsWith("SPARK") => key }
    keys.foreach(builder.environment().remove)
    val process = builder.start()

    val logger1 = new ProcessLogger(process.getInputStream)
    val logger2 = new ProcessLogger(process.getErrorStream)
    logger1.start()
    logger2.start()

    process.waitFor()

    while(!logger1.completed || !logger2.completed){
      Thread.sleep(100)
    }

    process.exitValue()
  }

  def findEventRDD(
    channelName: Option[String] = None,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    entityType: Option[String] = None,
    entityId: Option[String] = None,
    eventNames: Option[Seq[String]] = None,
    targetEntityType: Option[Option[String]] = None,
    targetEntityId: Option[Option[String]] = None
  )(sc: SparkContext): RDD[Event] = {
    toolbox.findEventRDD(
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
    channelName: Option[String] = None
  )(sc: SparkContext): Unit = {
    toolbox.writeEventRDD(events, appName, channelName)(sc)
  }

  def insertEvent(
    event: Event,
    channelName: Option[String] = None
  ): Unit = {
    toolbox.insertEvent(event, appName, channelName)
  }

  def insertEventBatch(
    events: Seq[Event],
    channelName: Option[String] = None
  ): Unit = {
    toolbox.insertEventBatch(events, appName, channelName)
  }

}
