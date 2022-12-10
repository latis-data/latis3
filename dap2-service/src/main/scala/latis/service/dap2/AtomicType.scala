package latis.service.dap2

import java.net.URL

sealed trait AtomicType[F,D] {
  def toDasValue(value:F): D
  def asDasString(value: F): String = this.toDasValue(value) match {
      case int: Int => int.toString
      case float: Double => f"${float.toDouble}%.6g"
      case string: String => "\"" +
        string.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
      case other => other.toString
  }
}

object AtomicType {
  final case object Byte extends AtomicType[Byte, Int] {
    override def toDasValue(value: Byte): Int = value.toInt
  }

  final case object Int16 extends AtomicType[Int, Int] {
    override def toDasValue(value: Int): Int = value
  }

  final case object UInt16 extends AtomicType[Int, Int] {
    override def toDasValue(value: Int): Int = value
  }

  final case object Int32 extends AtomicType[Int, Int] {
    override def toDasValue(value: Int): Int = value
  }

  final case object UInt32 extends AtomicType[Int, Int] {
    override def toDasValue(value: Int): Int = value
  }

  final case object Float32 extends AtomicType[Float, Double] {
    override def toDasValue(value: Float): Double = value.toDouble
  }

  final case object Float64 extends AtomicType[Double, Double] {
    override def toDasValue(value: Double): Double = value
  }

  final case object String extends AtomicType[String, String] {
    override def toDasValue(value: String): String = value
  }

  final case object Url extends AtomicType[URL, String] {
    override def toDasValue(value: URL): String = value.toString
  }
}
