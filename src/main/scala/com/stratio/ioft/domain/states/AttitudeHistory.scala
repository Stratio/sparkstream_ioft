package com.stratio.ioft.domain.states

import com.stratio.ioft.domain.measures.Attitude

case class AttitudeHistory(attitudes: Seq[(BigInt, Attitude)]) {
  def isEmpty: Boolean = attitudes.isEmpty
  def startsAt: Option[BigInt] = attitudes.headOption.map(_._1)
  def endsAt: Option[BigInt] = attitudes.lastOption.map(_._1)

  def attitudeAt(timestamp_ms: BigInt): Seq[(BigInt, Attitude)] =
    attitudes indexWhere { case (ts, _) => ts > timestamp_ms } match {
      case -1 => Seq()
      case 0 => Seq(attitudes.head)
      case n => attitudes.slice(n-1, n+1)
    }

}