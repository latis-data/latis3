package latis.data

import org.scalacheck.Gen
import org.scalacheck.Prop._
import org.scalatest.Inside.inside
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.Checkers

import latis.util.LatisException

class DatumSuite extends AnyFunSuite with Checkers {

  test("Equivalence of boolean construction via Data.fromValue() vs BooleanValue()") {
    check(forAll { (bool: Boolean) =>
      val boolVal = Data.BooleanValue(bool)

      Data.fromValue(bool) match {
        case Right(fromVal: Data.BooleanValue) =>
          (boolVal.toString == fromVal.toString) && (boolVal.asString == fromVal.asString) &&
            (boolVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of byte construction via Data.fromValue() vs ByteValue()") {
    check(forAll { (byte: Byte) =>
      val byteVal = Data.ByteValue(byte)

      Data.fromValue(byte) match {
        case Right(fromVal: Data.ByteValue) =>
          (byteVal.toString == fromVal.toString) && (byteVal.asString == fromVal.asString) &&
            (byteVal.asInt == fromVal.asInt) && (byteVal.asLong == fromVal.asLong) &&
            (byteVal.asDouble == fromVal.asDouble) && (byteVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of char construction via Data.fromValue() vs CharValue()") {
    check(forAll { (chr: Char) =>
      val charVal = Data.CharValue(chr)

      Data.fromValue(chr) match {
        case Right(fromVal: Data.CharValue) =>
          (charVal.toString == fromVal.toString) && (charVal.asString == fromVal.asString) &&
            (charVal.asInt == fromVal.asInt) && (charVal.asLong == fromVal.asLong) &&
            (charVal.asDouble == fromVal.asDouble) && (charVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of short construction via Data.fromValue() vs ShortValue()") {
    check(forAll { (short: Short) =>
      val shortVal = Data.ShortValue(short)

      Data.fromValue(short) match {
        case Right(fromVal: Data.ShortValue) =>
          (shortVal.toString == fromVal.toString) && (shortVal.asString == fromVal.asString) &&
            (shortVal.asInt == fromVal.asInt) && (shortVal.asLong == fromVal.asLong) &&
            (shortVal.asDouble == fromVal.asDouble) && (shortVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of int construction via Data.fromValue() vs IntValue()") {
    check(forAll { (int: Int) =>
      val intVal = Data.IntValue(int)

      Data.fromValue(int) match {
        case Right(fromVal: Data.IntValue) =>
          (intVal.toString == fromVal.toString) && (intVal.asString == fromVal.asString) &&
            (intVal.asInt == fromVal.asInt) && (intVal.asLong == fromVal.asLong) &&
            (intVal.asDouble == fromVal.asDouble) && (intVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of long construction via Data.fromValue() vs LongValue()") {
    check(forAll { (long: Long) =>
      val longVal = Data.LongValue(long)

      Data.fromValue(long) match {
        case Right(fromVal: Data.LongValue) =>
          (longVal.toString == fromVal.toString) && (longVal.asString == fromVal.asString) &&
            (longVal.asLong == fromVal.asLong) && (longVal.asDouble == fromVal.asDouble) &&
            (longVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of float construction via Data.fromValue() vs FloatValue()") {
    check(forAll { (float: Float) =>
      val floatVal = Data.FloatValue(float)

      Data.fromValue(float) match {
        case Right(fromVal: Data.FloatValue) =>
          (floatVal.toString == fromVal.toString) && (floatVal.asString == fromVal.asString) &&
            (floatVal.asDouble == fromVal.asDouble) && (floatVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of double construction via Data.fromValue() vs DoubleValue()") {
    check(forAll { (double: Double) =>
      val doubleVal = Data.DoubleValue(double)

      Data.fromValue(double) match {
        case Right(fromVal: Data.DoubleValue) =>
          (doubleVal.toString == fromVal.toString) && (doubleVal.asString == fromVal.asString) &&
            (doubleVal.asDouble == fromVal.asDouble) && (doubleVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of string construction via Data.fromValue() vs StringValue()") {
    check(forAll { (str: String) =>
      val strVal = Data.StringValue(str)

      Data.fromValue(str) match {
        case Right(fromVal: Data.StringValue) =>
          (strVal.toString == fromVal.toString) && (strVal.asString == fromVal.asString) &&
            (strVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of binary construction via Data.fromValue() vs BinaryValue()") {
    check(forAll { (bin: Array[Byte]) =>
      val binVal = Data.BinaryValue(bin)

      Data.fromValue(bin) match {
        case Right(fromVal: Data.BinaryValue) =>
          (binVal.toString == fromVal.toString) && (binVal.asString == fromVal.asString) &&
            (binVal.value sameElements fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of bigInt construction via Data.fromValue() vs BigIntValue()") {
    check(forAll { (bigInt: BigInt) =>
      val biVal = Data.BigIntValue(bigInt)

      Data.fromValue(bigInt) match {
        case Right(fromVal: Data.BigIntValue) =>
          (biVal.toString == fromVal.toString) && (biVal.asString == fromVal.asString) &&
            (biVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Equivalence of bigDecimal construction via Data.fromValue() vs BigDecimalValue()") {
    check(forAll { (bigDeci: BigDecimal) =>
      val bdVal = Data.BigDecimalValue(bigDeci)

      Data.fromValue(bigDeci) match {
        case Right(fromVal: Data.BigDecimalValue) =>
          (bdVal.toString == fromVal.toString) && (bdVal.asString == fromVal.asString) &&
            (bdVal.value == fromVal.value)
        case _ => false
      }
    })
  }

  test("Data.fromValue() generates an exception when called with null") {
    inside (Data.fromValue(null)) {
      case Left(LatisException(message, _)) =>
        message == "Can't make a Datum from the value null"
      case _ => false
    }
  }

  test("Data.fromValue() generates an exception when called with an unsupported type") {
    inside (Data.fromValue(List(1.0, 7.4, -3.5))) {
      case Left(LatisException(message, _)) =>
        message == "Can't make a Datum from the value List(1.0, 7.4, -3.5)"
      case _ => false
    }
  }

  test("Byte pattern matching (unapply)") {
    check(forAll { (byte: Byte) =>
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
    })
  }

  test("Char pattern matching (unapply)") {
    check(forAll { (chr: Char) =>
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
    })
  }

  test("Short pattern matching (unapply)") {
    check(forAll { (short: Short) =>
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
    })
  }

  test("Int pattern matching (unapply)") {
    check(forAll { (int: Int) =>
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
    })
  }

  test("Long pattern matching (unapply)") {
    check(forAll { (long: Long) =>
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
    })
  }

  test("Float pattern matching (unapply)") {
    check(forAll { (float: Float) =>
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
    })
  }

  test("Double pattern matching (unapply)") {
    check(forAll { (double: Double) =>
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
    })
  }

  test("String pattern matching (unapply)") {
    check(forAll { (str: String) =>
      val strVal = Data.StringValue(str)

      strVal match {
        case Text(x) => x == str
        case _ => false
      }
    })
  }

  test("BooleanValue value, asString, and toString") {
    check(forAll { (bool: Boolean) =>
      val boolVal = Data.BooleanValue(bool)

      boolVal.value == bool &&
        boolVal.asString == bool.toString &&
        boolVal.toString == s"BooleanValue($bool)"
    })
  }

  test("ByteValue value, asString, and toString") {
    check(forAll { (byte: Byte) =>
      val byteVal = Data.ByteValue(byte)

      byteVal.value == byte &&
        byteVal.asString == byte.toString &&
        byteVal.toString == s"ByteValue($byte)"
    })
  }

  test("CharValue value, asString, and toString") {
    check(forAll { (chr: Char) =>
      val charVal = Data.CharValue(chr)

      charVal.value == chr &&
        charVal.asString == chr.toString &&
        charVal.toString == s"CharValue($chr)"
    })
  }

  test("ShortValue value, asString, and toString") {
    check(forAll { (short: Short) =>
      val shortVal = Data.ShortValue(short)

      shortVal.value == short &&
        shortVal.asString == short.toString &&
        shortVal.toString == s"ShortValue($short)"
    })
  }

  test("IntValue value, asString, and toString") {
    check(forAll { (int: Int) =>
      val intVal = Data.IntValue(int)

      intVal.value == int &&
        intVal.asString == int.toString &&
        intVal.toString == s"IntValue($int)"
    })
  }

  test("LongValue value, asString, and toString") {
    check(forAll { (long: Long) =>
      val longVal = Data.LongValue(long)

      longVal.value == long &&
        longVal.asString == long.toString &&
        longVal.toString == s"LongValue($long)"
    })
  }

  test("FloatValue value, asString, and toString") {
    check(forAll { (float: Float) =>
      val floatVal = Data.FloatValue(float)

      floatVal.value == float &&
        floatVal.asString == float.toString &&
        floatVal.toString == s"FloatValue($float)"
    })
  }

  test("DoubleValue value, asString, and toString") {
    check(forAll { (double: Double) =>
      val doubleVal = Data.DoubleValue(double)

      doubleVal.value == double &&
        doubleVal.asString == double.toString &&
        doubleVal.toString == s"DoubleValue($double)"
    })
  }

  test("StringValue value, asString, and toString") {
    check(forAll { (str: String) =>
      val strVal = Data.StringValue(str)

      strVal.value == str &&
        strVal.asString == str &&
        strVal.toString == s"StringValue($str)"
    })
  }

  test("BinaryValue value, asString, and toString") {
    val gen: Gen[Int] = Gen.choose(0, 1000000)
    check(forAll(gen) { (len: Int) =>
      val bin: Array[Byte] = scala.util.Random.nextBytes(len)
      val binVal = Data.BinaryValue(bin)
      (binVal.value sameElements bin) &&
        binVal.asString == len.toString + " Byte Binary" &&
        binVal.toString == "BinaryValue(" + binVal.asString + ")"
    })
  }

  test("BigIntValue value, asString, and toString") {
    check(forAll { (bigInt: BigInt) =>
      val bigIntVal = Data.BigIntValue(bigInt)

      bigIntVal.value == bigInt &&
        bigIntVal.asString == bigInt.toString &&
        bigIntVal.toString == s"BigIntValue($bigInt)"
    })
  }

  test("BigDecimalValue value, asString, and toString") {
    check(forAll { (bigDeci: BigDecimal) =>
      val bigDeciVal = Data.BigDecimalValue(bigDeci)

      bigDeciVal.value == bigDeci &&
        bigDeciVal.asString == bigDeci.toString &&
        bigDeciVal.toString == s"BigDecimalValue($bigDeci)"
    })
  }

}
