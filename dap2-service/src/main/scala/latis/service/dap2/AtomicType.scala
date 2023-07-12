package latis.service.dap2

import java.net.URL

import latis.util.StringUtils.*

sealed trait AtomicType[F] {
  private val fmtInt: AnyVal => String = i => i.toString
  private val fmtFloat: Double => String = f => f"$f%.6g"
  private val fmtString: String => String = ensureQuotedAndEscaped

  def ofValue(value: F): F = value
  def asDasString(value: F): String = this.ofValue(value) match {
      case int: Byte => fmtInt(int)
      case int: Short => fmtInt(int)
      case int: Int => fmtInt(int)
      case int: Long => fmtInt(int)
      case float: Float => fmtFloat(float.toDouble)
      case float: Double => fmtFloat(float)
      case string: String => fmtString(string)
      case string: URL => fmtString(string.toString)
      case other => other.toString
  }
}

object AtomicType {
  case object Byte extends AtomicType[Byte]
  case object Int16 extends AtomicType[Short]
  case object UInt16 extends AtomicType[Int] {
    val max: Int = Integer.parseInt("FFFF",16)
    override def ofValue(value: Int): Int = value.min(max).max(0)
  }
  case object Int32 extends AtomicType[Int]
  case object UInt32 extends AtomicType[Long] {
    val max: Long = java.lang.Long.parseLong("FFFFFFFF",16)
    override def ofValue(value: Long): Long = value.min(max).max(0)
  }
  case object Float32 extends AtomicType[Float]
  case object Float64 extends AtomicType[Double]
  case object String extends AtomicType[String]
  case object Url extends AtomicType[URL]
}
