package latis.data

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.*

class DatumSuite extends ScalaCheckSuite {

  property("Data.fromValue(x) == BooleanValue(x)") {
    forAll { (bool: Boolean) =>
      val boolVal = Data.BooleanValue(bool)

      Data.fromValue(bool) match {
        case Right(fromVal: Data.BooleanValue) =>
          (boolVal.toString == fromVal.toString) && (boolVal.asString == fromVal.asString) &&
          (boolVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) == ByteValue(x)") {
    forAll { (byte: Byte) =>
      val byteVal = Data.ByteValue(byte)

      Data.fromValue(byte) match {
        case Right(fromVal: Data.ByteValue) =>
          (byteVal.toString == fromVal.toString) && (byteVal.asString == fromVal.asString) &&
            (byteVal.asInt == fromVal.asInt) && (byteVal.asLong == fromVal.asLong) &&
            (byteVal.asDouble == fromVal.asDouble) && (byteVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) == CharValue(x)") {
    forAll { (chr: Char) =>
      val charVal = Data.CharValue(chr)

      Data.fromValue(chr) match {
        case Right(fromVal: Data.CharValue) =>
          (charVal.toString == fromVal.toString) && (charVal.asString == fromVal.asString) &&
            (charVal.asInt == fromVal.asInt) && (charVal.asLong == fromVal.asLong) &&
            (charVal.asDouble == fromVal.asDouble) && (charVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) == ShortValue(x)") {
    forAll { (short: Short) =>
      val shortVal = Data.ShortValue(short)

      Data.fromValue(short) match {
        case Right(fromVal: Data.ShortValue) =>
          (shortVal.toString == fromVal.toString) && (shortVal.asString == fromVal.asString) &&
            (shortVal.asInt == fromVal.asInt) && (shortVal.asLong == fromVal.asLong) &&
            (shortVal.asDouble == fromVal.asDouble) && (shortVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) == IntValue(x)") {
    forAll { (int: Int) =>
      val intVal = Data.IntValue(int)

      Data.fromValue(int) match {
        case Right(fromVal: Data.IntValue) =>
          (intVal.toString == fromVal.toString) && (intVal.asString == fromVal.asString) &&
            (intVal.asInt == fromVal.asInt) && (intVal.asLong == fromVal.asLong) &&
            (intVal.asDouble == fromVal.asDouble) && (intVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) == LongValue(x)") {
    forAll { (long: Long) =>
      val longVal = Data.LongValue(long)

      Data.fromValue(long) match {
        case Right(fromVal: Data.LongValue) =>
          (longVal.toString == fromVal.toString) && (longVal.asString == fromVal.asString) &&
            (longVal.asLong == fromVal.asLong) && (longVal.asDouble == fromVal.asDouble) &&
            (longVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) == FloatValue(x)") {
    forAll { (float: Float) =>
      val floatVal = Data.FloatValue(float)

      Data.fromValue(float) match {
        case Right(fromVal: Data.FloatValue) =>
          (floatVal.toString == fromVal.toString) && (floatVal.asString == fromVal.asString) &&
            (floatVal.asDouble == fromVal.asDouble) && (floatVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) vs DoubleValue(x)") {
    forAll { (double: Double) =>
      val doubleVal = Data.DoubleValue(double)

      Data.fromValue(double) match {
        case Right(fromVal: Data.DoubleValue) =>
          (doubleVal.toString == fromVal.toString) && (doubleVal.asString == fromVal.asString) &&
            (doubleVal.asDouble == fromVal.asDouble) && (doubleVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) == StringValue(x)") {
    forAll { (str: String) =>
      val strVal = Data.StringValue(str)

      Data.fromValue(str) match {
        case Right(fromVal: Data.StringValue) =>
          (strVal.toString == fromVal.toString) && (strVal.asString == fromVal.asString) &&
            (strVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) == BinaryValue(x)") {
    forAll { (bin: Array[Byte]) =>
      val binVal = Data.BinaryValue(bin)

      Data.fromValue(bin) match {
        case Right(fromVal: Data.BinaryValue) =>
          (binVal.toString == fromVal.toString) && (binVal.asString == fromVal.asString) &&
            (binVal.value sameElements fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) == BigIntValue(x)") {
    forAll { (bigInt: BigInt) =>
      val biVal = Data.BigIntValue(bigInt)

      Data.fromValue(bigInt) match {
        case Right(fromVal: Data.BigIntValue) =>
          (biVal.toString == fromVal.toString) && (biVal.asString == fromVal.asString) &&
            (biVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  property("Data.fromValue(x) == BigDecimalValue(x)") {
    forAll { (bigDeci: BigDecimal) =>
      val bdVal = Data.BigDecimalValue(bigDeci)

      Data.fromValue(bigDeci) match {
        case Right(fromVal: Data.BigDecimalValue) =>
          (bdVal.toString == fromVal.toString) && (bdVal.asString == fromVal.asString) &&
            (bdVal.value == fromVal.value)
        case _ => false
      }
    }
  }

  test("Data.fromValue() returns Left when called with null") {
    assert(Data.fromValue(null).isLeft)
  }

  test("Data.fromValue() returns Left when called with an unsupported type") {
    assert(Data.fromValue(List(1.0, 7.4, -3.5)).isLeft)
  }

  property("Byte pattern matching (unapply)") {
    forAll { (byte: Byte) =>
      val byteVal = Data.ByteValue(byte)

      val idMatch = byteVal match {
        case IndexDatum(x) => x == byte
        case _ => false
      }

      val intMatch = byteVal match {
        case Integer(x) => x == byte
        case _ => false
      }

      val numMatch = byteVal match {
        case Number(x) => x == byte
        case _ => false
      }

      idMatch && intMatch && numMatch
    }
  }

  property("Char pattern matching (unapply)") {
    forAll { (chr: Char) =>
      val charVal = Data.CharValue(chr)

      val idMatch = charVal match {
        case IndexDatum(x) => x == chr
        case _ => false
      }

      val intMatch = charVal match {
        case Integer(x) => x == chr
        case _ => false
      }

      val numMatch = charVal match {
        case Number(x) => x == chr
        case _ => false
      }

      val txtMatch = charVal match {
        case Text(x) => x == chr.toString
        case _ => false
      }

      idMatch && intMatch && numMatch && txtMatch
    }
  }

  property("Short pattern matching (unapply)") {
    forAll { (short: Short) =>
      val shortVal = Data.ShortValue(short)

      val idMatch = shortVal match {
        case IndexDatum(x) => x == short
        case _ => false
      }

      val intMatch = shortVal match {
        case Integer(x) => x == short
        case _ => false
      }

      val numMatch = shortVal match {
        case Number(x) => x == short
        case _ => false
      }

      idMatch && intMatch && numMatch
    }
  }

  property("Int pattern matching (unapply)") {
    forAll { (int: Int) =>
      val intVal = Data.IntValue(int)

      val idMatch = intVal match {
        case IndexDatum(x) => x == int
        case _ => false
      }

      val intMatch = intVal match {
        case Integer(x) => x == int
        case _ => false
      }

      val numMatch = intVal match {
        case Number(x) => x == int
        case _ => false
      }

      idMatch && intMatch && numMatch
    }
  }

  property("Long pattern matching (unapply)") {
    forAll { (long: Long) =>
      val longVal = Data.LongValue(long)

      val intMatch = longVal match {
        case Integer(x) => x == long
        case _ => false
      }

      val numMatch = longVal match {
        case Number(x) => x == long
        case _ => false
      }

      intMatch && numMatch
    }
  }

  property("Float pattern matching (unapply)") {
    forAll { (float: Float) =>
      val floatVal = Data.FloatValue(float)

      val reMatch = floatVal match {
        case Real(x) => x == float
        case _ => false
      }

      val numMatch = floatVal match {
        case Number(x) => x == float
        case _ => false
      }

      reMatch && numMatch
    }
  }

  property("Double pattern matching (unapply)") {
    forAll { (double: Double) =>
      val doubleVal = Data.DoubleValue(double)

      val reMatch = doubleVal match {
        case Real(x) => x == double
        case _ => false
      }

      val numMatch = doubleVal match {
        case Number(x) => x == double
        case _ => false
      }

      reMatch && numMatch
    }
  }

  property("String pattern matching (unapply)") {
    forAll { (str: String) =>
      val strVal = Data.StringValue(str)

      strVal match {
        case Text(x) => x == str
        case _ => false
      }
    }
  }

  property("BooleanValue value, asString, and toString") {
    forAll { (bool: Boolean) =>
      val boolVal = Data.BooleanValue(bool)

      boolVal.value == bool &&
        boolVal.asString == bool.toString &&
        boolVal.toString == s"BooleanValue($bool)"
    }
  }

  property("ByteValue value, asString, and toString") {
    forAll { (byte: Byte) =>
      val byteVal = Data.ByteValue(byte)

      byteVal.value == byte &&
        byteVal.asString == byte.toString &&
        byteVal.toString == s"ByteValue($byte)"
    }
  }

  property("CharValue value, asString, and toString") {
    forAll { (chr: Char) =>
      val charVal = Data.CharValue(chr)

      charVal.value == chr &&
        charVal.asString == chr.toString &&
        charVal.toString == s"CharValue($chr)"
    }
  }

  property("ShortValue value, asString, and toString") {
    forAll { (short: Short) =>
      val shortVal = Data.ShortValue(short)

      shortVal.value == short &&
        shortVal.asString == short.toString &&
        shortVal.toString == s"ShortValue($short)"
    }
  }

  property("IntValue value, asString, and toString") {
    forAll { (int: Int) =>
      val intVal = Data.IntValue(int)

      intVal.value == int &&
        intVal.asString == int.toString &&
        intVal.toString == s"IntValue($int)"
    }
  }

  property("LongValue value, asString, and toString") {
    forAll { (long: Long) =>
      val longVal = Data.LongValue(long)

      longVal.value == long &&
        longVal.asString == long.toString &&
        longVal.toString == s"LongValue($long)"
    }
  }

  property("FloatValue value, asString, and toString") {
    forAll { (float: Float) =>
      val floatVal = Data.FloatValue(float)

      floatVal.value == float &&
        floatVal.asString == float.toString &&
        floatVal.toString == s"FloatValue($float)"
    }
  }

  property("DoubleValue value, asString, and toString") {
    forAll { (double: Double) =>
      val doubleVal = Data.DoubleValue(double)

      doubleVal.value == double &&
        doubleVal.asString == double.toString &&
        doubleVal.toString == s"DoubleValue($double)"
    }
  }

  property("StringValue value, asString, and toString") {
    forAll { (str: String) =>
      val strVal = Data.StringValue(str)

      strVal.value == str &&
        strVal.asString == str &&
        strVal.toString == s"StringValue($str)"
    }
  }

  property("BinaryValue value, asString, and toString") {
    val gen: Gen[Int] = Gen.choose(0, 1000000)

    forAll(gen) { (len: Int) =>
      val bin: Array[Byte] = scala.util.Random.nextBytes(len)
      val binVal = Data.BinaryValue(bin)
      (binVal.value sameElements bin) &&
        binVal.asString == len.toString + " Byte Binary" &&
        binVal.toString == "BinaryValue(" + binVal.asString + ")"
    }
  }

  property("BigIntValue value, asString, and toString") {
    forAll { (bigInt: BigInt) =>
      val bigIntVal = Data.BigIntValue(bigInt)

      bigIntVal.value == bigInt &&
        bigIntVal.asString == bigInt.toString &&
        bigIntVal.toString == s"BigIntValue($bigInt)"
    }
  }

  property("BigDecimalValue value, asString, and toString") {
    forAll { (bigDeci: BigDecimal) =>
      val bigDeciVal = Data.BigDecimalValue(bigDeci)

      bigDeciVal.value == bigDeci &&
        bigDeciVal.asString == bigDeci.toString &&
        bigDeciVal.toString == s"BigDecimalValue($bigDeci)"
    }
  }
}
