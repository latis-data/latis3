package latis.ops.anomalies

/**
 * Defines the definitions of "anomaly" that can be used in the DetectAnomalies Operation.
 */
sealed trait AnomalyDef {

  /**
   * Returns the String literal representing this anomaly definition
   */
  def asString: String
}

/** Defines anomalies using standard deviations from the data's mean */
final case object Errors extends AnomalyDef {
  override def asString: String = "errors"
}

/** Defines anomalies using standard deviations from the mean of the errors */
final case object Std extends AnomalyDef {
  override def asString: String = "std"
}

/** Defines anomalies using nonparametric dynamic thresholding */
final case object Dynamic extends AnomalyDef {
  override def asString: String = "dynamic"
}
