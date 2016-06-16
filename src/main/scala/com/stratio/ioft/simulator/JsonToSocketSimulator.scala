package com.stratio.ioft.simulator

import java.io.PrintWriter
import java.net.ServerSocket
import java.util
import java.util.Map

import com.fasterxml.jackson.core.`type`._
import com.fasterxml.jackson.databind.ObjectMapper

import scala.io.Source

object JsonToSocketSimulator extends App {

  require(args.length == 1,
    """
      |Usage: JsonToSocketSimulator /path/to/flightData.jsons
      |
      |Example: JsonToSocketSimulator samples/dronestream_withcontrols.jsons
    """.stripMargin)

  val incomingFile = args.apply(0)

  val server = new ServerSocket(7891)
  println(s"Server Address: ${server.getLocalSocketAddress}")
  val connection  = server.accept
  println(s"Connection from: ${connection.getRemoteSocketAddress}")
  val out = new PrintWriter(connection.getOutputStream)

  init()

  close()

  def init() = {
    var jsonMap = new util.HashMap[String, Object]()
    val mapper = new ObjectMapper
    var prevTimestamp = Long.MaxValue
    var currentTimestamp = Long.MaxValue

    for (line <- Source.fromFile(incomingFile).getLines.zipWithIndex) {
      jsonMap = mapper.readValue(line._1, new TypeReference[Map[String, Object]]() {})
      println(s"LINE ${line._2} = gcs_timestamp_ms: ${jsonMap.get("gcs_timestamp_ms")}")
      currentTimestamp = jsonMap.get("gcs_timestamp_ms").asInstanceOf[Long]
      if (prevTimestamp != Long.MaxValue) {
        //println(s"Waiting ${currentTimestamp - prevTimestamp} ms")
        Thread.sleep(currentTimestamp - prevTimestamp)
      }
      prevTimestamp = currentTimestamp
      write(line._1)
    }
  }

  def write(line: String) = {
    out.write(s"$line${System.lineSeparator}")
    out.flush
  }

  def close() = {
    out.flush
    server.close
  }

}

