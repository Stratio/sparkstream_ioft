package com.stratio.ioft.streaming.drivers

import com.stratio.ioft.serialization.json4s.librePilotSerializers
import com.stratio.ioft.domain.LibrePilot.Entry
import com.stratio.ioft.domain._
import com.stratio.ioft.domain.measures.Acceleration
import com.stratio.ioft.settings.IOFTConfig
import com.stratio.ioft.streaming.transformations.Detectors._
import com.stratio.ioft.streaming.transformations.Sources._
import com.stratio.ioft.util.PresentationTools
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

object JustOutliersBasedBumpDetection extends App
  with IOFTConfig
  with PresentationTools {

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf() setMaster(sparkConfig.getString("master")) setAppName(sparkConfig.getString("appName"))

  val sc = new StreamingContext(conf, Milliseconds(sparkStreamingConfig.getLong("batchDuration")))

  val droneId: DroneIdType = "drone01"

  val rawInputStream = sc.socketTextStream(
    sourceConfig.getString("host"), sourceConfig.getInt("port")
  ) map (json => droneId -> json)

  implicit val formats = DefaultFormats ++ librePilotSerializers

  val bumpInterval = Seconds(5)

  val entriesStream = rawInputStream.mapValues(parse(_).extract[Entry])
  val entriesWindowedStream = entriesStream.window(bumpInterval, bumpInterval)

  val accelWindowedStream = accelerationStream(entriesWindowedStream)

  val bumpStream = averageOutlierBumpDetector(
    accelWindowedStream.mapValues { case (ts, Acceleration(x,y,z)) => ts -> z }, 5.0, 1.0
  )

  val groupedBumps = bumpStream map { case (id, (ts, accel)) => (id, ts/2000) -> (ts, accel) } reduceByKey { (a, b) =>
    Seq(a, b).maxBy(contester => math.abs(contester._2))
  } map {
    case ((id: DroneIdType, _), (ts: TimestampMS, accel: Double)) => (id, ts -> accel)
  }

  groupedBumps.foreachRDD(_.foreach(event => printDroneEvent("BUMP DETECTED", event)))

  sc.start()
  sc.awaitTermination


}
