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


case class RunningInfo(process: Process, port: Int)

case class PIOApp(
  toolbox: PIOToolbox,
  appName: String,
  url: String,
  dir: File,
  algorithms: String
){
  private implicit val formats = Serialization.formats(NoTypeHints)
  private var runningInfo: RunningInfo = null

  def build(): Int = {
    println(s"[${appName}] Compiling template...")
    executeCommand(Seq(s"${toolbox.pioHome}/bin/pio", "build"), true).exitValue()
  }

  def open(): Unit = {
    executeCommand(Seq("open", dir.getAbsolutePath), true)
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

    println(s"[${appName}] Training model...")
    executeCommand(Seq(s"${toolbox.pioHome}/bin/pio", "train"), true).exitValue()
  }

  def deploy(port: Int = 8000): Unit = {
    if(runningInfo != null){
      println(s"[${appName}] Service is running.")
    } else {
      println(s"[${appName}] Deploying service...")
      val process = executeCommand(Seq(s"${toolbox.pioHome}/bin/pio", "deploy", "--port", port.toString), false)
      runningInfo = RunningInfo(process, port)
      println(s"[${appName}] Service has been started on the port: ${port}")
    }
  }

  def shutdown(): Unit = {
    if(runningInfo == null){
      println(s"[${appName}] Service is not running.")
    } else {
      println(s"[${appName}] Shutdown service...")
      runningInfo.process.destroy()
      runningInfo = null
    }
  }

  def status(): Boolean = {
    if(runningInfo == null){
      println(s"[${appName}] Service is not running")
      false
    } else {
      println(s"[${appName}] Service is running on the port ${runningInfo.port}")
      true
    }
  }

  private def executeCommand(command: Seq[String], waitForCompletion: Boolean): Process = {
    val builder = new ProcessBuilder(command: _*).directory(dir)

    // Remove SPARK keys
    val keys = builder.environment().asScala.collect { case (key, value) if key.startsWith("SPARK") => key }
    keys.foreach(builder.environment().remove)
    val process = builder.start()

    val logger1 = new ProcessLogger(process.getInputStream)
    val logger2 = new ProcessLogger(process.getErrorStream)
    logger1.start()
    logger2.start()

    if(waitForCompletion){
      process.waitFor()

      while(!logger1.completed || !logger2.completed){
        Thread.sleep(100)
      }
    }

    process
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
