package com.stratio.ioft.streaming.transformations

import com.stratio.ioft.domain.LibrePilot.Entry
import com.stratio.ioft.domain._
import org.apache.spark.streaming.dstream.DStream

/**
  * Template for creating a pattern detector.
  */
object TransformationTemplate {

  /**
    * Dummy method that represents the input parameter and a possible returning type. The returning type should be a
    * [[DStream]] with a [[Tuple2]] where the first element should be the drone identifier and the second element can be
    * a Tuple of any number of elements.
    * @param entriesStream [[DStream]] with streaming events associated to the drone identifier.
    * @return [[DStream]] with the events that meet the pattern logic for every drone identifier.
    */
  def detectPattern(entriesStream: DStream[(DroneIdType, Entry)]): DStream[(DroneIdType, (Any, Any, Any))] =
    entriesStream map { event =>
      (event._1, (event._2.gcs_timestamp_ms, event._2.fields.head.name, event._2.fields.head.values.head.value))
    }

}
