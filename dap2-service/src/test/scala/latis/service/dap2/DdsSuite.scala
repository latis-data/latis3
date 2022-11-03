package latis.service.dap2

import munit.CatsEffectSuite

import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model._
import latis.service.dap2.Dds._
import latis.util.Identifier.IdentifierStringContext

class DdsSuite extends CatsEffectSuite {
  test("correct AtomicType toString representation") {
    assertEquals(Byte.toString, "Byte")
    assertEquals(Int16.toString, "Int16")
    assertEquals(UInt16.toString, "UInt16")
    assertEquals(Int32.toString, "Int32")
    assertEquals(UInt32.toString, "UInt32")
    assertEquals(Float32.toString, "Float32")
    assertEquals(Float64.toString, "Float64")
    assertEquals(String.toString, "String")
    assertEquals(Url.toString, "Url")
  }

  test("correct AtomicDecl toDoc representation") {
    assertEquals(AtomicDecl(id"testByte", Byte).toDoc.render(1), "Byte testByte;")
    assertEquals(AtomicDecl(id"testInt", UInt32).toDoc.render(1), "UInt32 testInt;")
    assertEquals(AtomicDecl(id"testFloat", Float32).toDoc.render(1), "Float32 testFloat;")
    assertEquals(AtomicDecl(id"testString", String).toDoc.render(1), "String testString;")
  }

  val decl1: AtomicDecl = AtomicDecl(id"byte1", Byte)
  val decl2: AtomicDecl = AtomicDecl(id"byte2", Byte)
  val decl3 = StructureDecl(id"innerStruct", List(AtomicDecl(id"byte3", Byte), AtomicDecl(id"byte4", Byte)))
  val decl4 = SequenceDecl(id"innerSeq", List(AtomicDecl(id"byte5", Byte), AtomicDecl(id"byte6", Byte)))

  test("correct StructureDecl toDoc representation") {
    val struct = StructureDecl(id"testStruct", List(decl1, decl2, decl3, decl4))
    assertEquals(
      struct.toDoc.render(1),
      """Structure {
        |  Byte byte1;
        |  Byte byte2;
        |  Structure {
        |    Byte byte3;
        |    Byte byte4;
        |  } innerStruct;
        |  Sequence {
        |    Byte byte5;
        |    Byte byte6;
        |  } innerSeq;
        |} testStruct;""".stripMargin.replace("\r", "")
    )
  }

  test("correct SequenceDecl toDoc representation") {
    val seq = SequenceDecl(id"testSeq", List(decl1, decl2, decl3, decl4))
    assertEquals(
      seq.toDoc.render(1),
      """Sequence {
        |  Byte byte1;
        |  Byte byte2;
        |  Structure {
        |    Byte byte3;
        |    Byte byte4;
        |  } innerStruct;
        |  Sequence {
        |    Byte byte5;
        |    Byte byte6;
        |  } innerSeq;
        |} testSeq;""".stripMargin.replace("\r", "")
    )
  }

  test("correct Dds asString representation") {
    val dds = Dds(id"testDDS", List(decl1, decl2, decl3, decl4))
    assertEquals(
      dds.asString,
      """Dataset {
        |  Byte byte1;
        |  Byte byte2;
        |  Structure {
        |    Byte byte3;
        |    Byte byte4;
        |  } innerStruct;
        |  Sequence {
        |    Byte byte5;
        |    Byte byte6;
        |  } innerSeq;
        |} testDDS;""".stripMargin.replace("\r", "")
    )
  }

  test("correct fromDatatype Scalar Dds type declarations") {
    assert(fromDataType(Scalar(id"boolean1", BooleanValueType)).isLeft)
    assertEquals(fromDataType(Scalar(id"byte1", ByteValueType)).getOrElse(null).toDoc.render(1), "Byte byte1;")
    assert(fromDataType(Scalar(id"char1", CharValueType)).isLeft)
    assertEquals(fromDataType(Scalar(id"short1", ShortValueType)).getOrElse(null).toDoc.render(1), "Int16 short1;")
    assertEquals(fromDataType(Scalar(id"int1", IntValueType)).getOrElse(null).toDoc.render(1), "Int32 int1;")
    assert(fromDataType(Scalar(id"long1", LongValueType)).isLeft)
    assertEquals(fromDataType(Scalar(id"float1", FloatValueType)).getOrElse(null).toDoc.render(1), "Float32 float1;")
    assertEquals(fromDataType(Scalar(id"double1", DoubleValueType)).getOrElse(null).toDoc.render(1), "Float64 double1;")
    assert(fromDataType(Scalar(id"bin1", BinaryValueType)).isLeft)
    assertEquals(fromDataType(Scalar(id"str1", StringValueType)).getOrElse(null).toDoc.render(1), "String str1;")
    assert(fromDataType(Scalar(id"big1", BigIntValueType)).isLeft)
    assert(fromDataType(Scalar(id"big2", BigDecimalValueType)).isLeft)
  }

  val s1 = Scalar(id"s1", StringValueType)
  val s2 = Scalar(id"s2", DoubleValueType)
  val t1 = Tuple.fromSeq(id"t1", Seq(s1, s2)).getOrElse(null)
  val s3 = Scalar(id"s3", ByteValueType)

  test("correct fromDatatype Tuple Dds type declarations") {
    val t2 = Tuple.fromSeq(id"t2", Seq[DataType](t1, s3)).getOrElse(null)
    assertEquals(
      fromDataType(t2).getOrElse(null).toDoc.render(1),
      """Structure {
        |  Structure {
        |    String s1;
        |    Float64 s2;
        |  } t1;
        |  Byte s3;
        |} t2;""".stripMargin.replace("\r", "")
    )
  }

  test("correct fromDatatype Function Dds type declarations") {
    val f1 = Function.from(s3, t1).getOrElse(null)
    assertEquals(
      fromDataType(f1).getOrElse(null).toDoc.render(1),
      """Sequence {
        |  Byte s3;
        |  Structure {
        |    String s1;
        |    Float64 s2;
        |  } t1;
        |} unknown;""".stripMargin.replace("\r", "")
    )
  }

  test("correct fromDataset Dds creation") {
    val tuple = Tuple.fromSeq(Seq[DataType](Scalar(id"irradiance", DoubleValueType), Scalar(id"uncertainty", DoubleValueType)))
    val inner = Function.from(
      id"spectrum",
      Scalar(id"wavelength", DoubleValueType),
      tuple.getOrElse(null)
    ).getOrElse(null)
    val dataset = new MemoizedDataset(
      Metadata(id"nrl2_ssi_P1Y"),
      Function.from(
        id"samples",
        Scalar(id"time", DoubleValueType),
        inner
      ).getOrElse(null),
      null
    )
    assertEquals(
      fromDataset(dataset).getOrElse(null).asString,
      """Dataset {
        |  Sequence {
        |    Float64 time;
        |    Sequence {
        |      Float64 wavelength;
        |      Structure {
        |        Float64 irradiance;
        |        Float64 uncertainty;
        |      } unknown;
        |    } spectrum;
        |  } samples;
        |} nrl2_ssi_P1Y;""".stripMargin.replace("\r", "")
    )
  }
}
