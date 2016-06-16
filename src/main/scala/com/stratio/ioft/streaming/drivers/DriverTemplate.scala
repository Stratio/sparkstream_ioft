package com.stratio.ioft.streaming.drivers

import com.stratio.ioft.domain.LibrePilot.Entry
import com.stratio.ioft.domain._
import com.stratio.ioft.persistence.CassandraPersistence._
import com.stratio.ioft.persistence.PrimaryKey
import com.stratio.ioft.serialization.json4s._
import com.stratio.ioft.settings.IOFTConfig
import com.stratio.ioft.streaming.transformations.TransformationTemplate._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
  * This Scala object is the template to generate an application for every exercise of the challenge. The main idea is
  * to keep the code included here and include methods where is indicated.
  * Configuration parameters of Spark, Websockets and Cassandra can be set up in the file located at
  * resources/settings.conf
  */
object DriverTemplate extends App with IOFTConfig {

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  // Create configuration according to the settings
  val conf = new SparkConf() setMaster(sparkConfig.getString("master")) setAppName(sparkConfig.getString("appName"))

  // Create Streaming Context with the configuration created above. The second parameter is the time interval at which
  // streaming data will be divided into batches.
  // For more information, visit http://spark.apache.org/docs/latest/streaming-programming-guide.html
  val sc = new StreamingContext(conf, Milliseconds(sparkStreamingConfig.getLong("batchDuration")))

  val droneId: DroneIdType = "drone01"

  // Socket Text Stream is one of the inputs that Spark Streaming provides. Data is received as bytes encoded with UTF8.
  // A map is applied in order to create a DStream associating the drone identifier with the text received.
  val rawInputStream = sc.socketTextStream(
    sourceConfig.getString("host"), sourceConfig.getInt("port")
  ) map (json => droneId -> json)

  implicit val formats = DefaultFormats ++ librePilotSerializers

  // Deserialization converting the text in JSON format to a class with the original format from Libre Pilot
  val entriesStream = rawInputStream.mapValues(parse(_).extract[Entry])

  ///////////////// START YOUR CODE HERE!
  // From this point, you should apply transformations and operations over entriesStream in order to detect the required
  // flight patterns. The transformations should be follow the structure of
  // com.stratio.ioft.streaming.transformations.TransformationTemplate
  val result = detectPattern(entriesStream)

  // Once you have the result of every interaction, you can do what you wish with it and keep the results in the best
  // way you think.
  // OPTIONALLY: You can persist the data easily in Cassandra using the next method:
  val tableName = "myPattern"
  val cols = Array("id", "timestamp", "event", "value")
  result.map(pattern => (pattern._1, pattern._2._1, pattern._2._2, pattern._2._3)).foreachRDD(rdd => {
    if (!rdd.isEmpty){
      createTable(s"$tableName", cols, PrimaryKey(Array("id"), Array("timestamp")), rdd.take(1).head)
      rdd.foreach(x => {
        persist(s"$tableName", cols, x)
      })
    }
  })
  // Note: Created tables will belong to the keyspace ioft

  ///////////////// END OF YOUR CODE!

  // Start the Streaming context. This makes to execute all the operations that makes use of the Streaming Context in a
  // loop fashion.
  sc.start()
  sc.awaitTermination
}
