package com.github.takezoe.predictionio.toolbox

import java.io.File

import org.apache.commons.io.FileUtils
import org.json4s.NoTypeHints
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.JsonMethods.parse
import scala.collection.JavaConverters._

case class Algorithm(name: String, params: Map[String, Any])

case class PIOApp(
  pioHome: String,
  appName: String,
  url: String,
  dir: File,
  algorithms: String
){
  private implicit val formats = Serialization.formats(NoTypeHints)

  def build(): Int = {
    println("Compiling application...")
    executeCommand(Seq(s"$pioHome/bin/pio", "build"))
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
    executeCommand(Seq(s"$pioHome/bin/pio", "train"))
  }

  def deploy(): Int = {
    println("Deploying service...")
    executeCommand(Seq(s"$pioHome/bin/pio", "deploy"))
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

  // TODO How to shutdown deployed API?
}
