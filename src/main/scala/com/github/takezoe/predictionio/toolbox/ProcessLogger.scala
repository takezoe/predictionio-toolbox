package com.github.takezoe.predictionio.toolbox

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

class ProcessLogger(in: InputStream) extends Thread {

  var completed = false

  override def run(): Unit = {
    val reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
    while(!completed){
      val line = reader.readLine()
      if(line == null){
        completed = true
      } else {
        //println(line)
        Thread.sleep(100)
      }
    }
  }

}
