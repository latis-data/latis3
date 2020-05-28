package latis.ops.anomalies

/**
 * Defines the three definitions of "anomaly" that can be used in the DetectAnomalies Operation.
 */
sealed trait AnomalyDef {

  /**
   * Returns the String literal representing this anomaly definition
   */
  def asString: String
}

final case object Errors extends AnomalyDef {
  override def asString: String = "errors"
}
final case object Std extends AnomalyDef {
  override def asString: String = "std"
}
final case object Dynamic extends AnomalyDef {
  override def asString: String = "dynamic"
}
