package latis.service.dap2

import cats.data.NonEmptyList
import munit.CatsEffectSuite

import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model._
import latis.service.dap2.AtomicType._
import latis.service.dap2.Das._
import latis.time.Time
import latis.util.Identifier.IdentifierStringContext

class DasSuite extends CatsEffectSuite {
  val byteAttr: Attribute[Byte.type,Byte,Int] = Attribute(id"testByte", Byte, NonEmptyList(42, List()))
  val uintAttr: Attribute[UInt32.type,Int,Int] = Attribute(id"testInt", UInt32, NonEmptyList(42, List()))
  val floatAttr: Attribute[Float32.type,Float,Double] = Attribute(id"testFloat", Float32, NonEmptyList(13.14159265359f, List()))
  val stringAttr: Attribute[String.type,String,String] = Attribute(id"testString", String, NonEmptyList("str1", List()))
  val stringsAttr: Attribute[String.type,String,String] = Attribute(id"testStrings", String, NonEmptyList("str1", List("str2", "str3")))
  val itemAttrCont: ItemAttributeCont = ItemAttributeCont(id"items", List(byteAttr, uintAttr, floatAttr, stringAttr, stringsAttr))
  val itemAttrStr: String =
    """items {
      |  Byte testByte 42;
      |  UInt32 testInt 42;
      |  Float32 testFloat 13.1416;
      |  String testString "str1";
      |  String testStrings "str1", "str2", "str3";
      |}""".stripMargin.replace("\r", "")
  val nestedAttrCont: NestedAttributeCont = NestedAttributeCont(
    id"items",
    List(
      ItemAttributeCont(id"byte", List(byteAttr)),
      NestedAttributeCont(
        id"stuff",
        List(
          ItemAttributeCont(id"numbers", List(uintAttr, floatAttr)),
          NestedAttributeCont(id"strings", List(ItemAttributeCont(id"one", List(stringAttr)), ItemAttributeCont(id"many", List(stringsAttr)))),
          ItemAttributeCont(id"nothing", List()),
          NestedAttributeCont(id"more_nothing", List())
        ))))
  val nestedAttrStr: String =
    """items {
      |  byte {
      |    Byte testByte 42;
      |  }
      |  stuff {
      |    numbers {
      |      UInt32 testInt 42;
      |      Float32 testFloat 13.1416;
      |    }
      |    strings {
      |      one {
      |        String testString "str1";
      |      }
      |      many {
      |        String testStrings "str1", "str2", "str3";
      |      }
      |    }
      |    nothing {
      |    ?
      |    }
      |    more_nothing {
      |    ?
      |    }
      |  }
      |}""".stripMargin.replace("\r", "").replace("?", "  ")

  test("correct Attribute toDoc representation") {
    assertEquals(byteAttr.toDoc.render(1), "Byte testByte 42;")
    assertEquals(uintAttr.toDoc.render(1), "UInt32 testInt 42;")
    assertEquals(floatAttr.toDoc.render(1), "Float32 testFloat 13.1416;")
    assertEquals(stringAttr.toDoc.render(1), "String testString \"str1\";")
    assertEquals(stringsAttr.toDoc.render(1), "String testStrings \"str1\", \"str2\", \"str3\";")
  }

  test("correct ItemAttributeCont toDoc representation") {
    assertEquals(ItemAttributeCont(id"empty", List()).toDoc.render(1), "empty {\n  \n}")
    assertEquals(
      itemAttrCont.toDoc.render(1),
      itemAttrStr
    )
  }

  test("correct NestedAttributeCont toDoc representation") {
    assertEquals(NestedAttributeCont(id"empty", List()).toDoc.render(1), "empty {\n  \n}")
    assertEquals(
      nestedAttrCont.toDoc.render(1),
      nestedAttrStr
    )
  }

  test("correct Das asString representation") {
    assertEquals(Das(List()).asString, "Attributes {\n  \n}")
    val das = Das(List(itemAttrCont, nestedAttrCont))
    assertEquals(
      das.asString,
      "Attributes {\n  " + itemAttrStr.replace("\n", "\n  ") + "\n  " +
        nestedAttrStr.replace("\n", "\n  ") + "\n}"
    )
  }

  test("correct fromDatatype Scalar attributes") {
    val scalar = Scalar(id"test", StringValueType)
    assertEquals(
      fromDataType(scalar).toDoc.render(1),
    """test {
        |  String type "string";
        |}""".stripMargin.replace("\r", "")
    )
  }

  val s1 = Scalar(id"s1", StringValueType)
  val s2 = Scalar(id"s2", DoubleValueType)
  val t1 = Tuple.fromSeq(id"t1", Seq(s1, s2)).getOrElse(???)
  val s3 = Scalar(id"s3", ByteValueType)
  test("correct fromDatatype Tuple attributes") {
    val t2 = Tuple.fromSeq(id"t2", Seq[DataType](t1, s3)).getOrElse(???)
    assertEquals(
      fromDataType(t2).toDoc.render(1),
      """t2 {
        |  t1 {
        |    s1 {
        |      String type "string";
        |    }
        |    s2 {
        |      String type "double";
        |    }
        |  }
        |  s3 {
        |    String type "byte";
        |  }
        |}""".stripMargin.replace("\r", "")
    )
  }

  test("correct fromDatatype Function attributes") {
    val f1 = Function.from(s3, t1).getOrElse(???)
    assertEquals(
      fromDataType(f1).toDoc.render(1),
      """unknown {
        |  s3 {
        |    String type "byte";
        |  }
        |  t1 {
        |    s1 {
        |      String type "string";
        |    }
        |    s2 {
        |      String type "double";
        |    }
        |  }
        |}""".stripMargin.replace("\r", "")
    )
    assertEquals(
      fromDataType(f1, true).toDoc.render(1),
      """samples {
        |  s3 {
        |    String type "byte";
        |  }
        |  t1 {
        |    s1 {
        |      String type "string";
        |    }
        |    s2 {
        |      String type "double";
        |    }
        |  }
        |}""".stripMargin.replace("\r", "")
    )
  }

  test("correct fromDataset Das creation") {
    val irradiance = Scalar.fromMetadata(
      Metadata(("id", "irradiance"), ("type", "double"), ("missingValue", "-99.0"), ("units", "W/m^2/nm"))
    ).getOrElse(???)
    val uncertainty = Scalar.fromMetadata(
      Metadata(("id", "uncertainty"), ("type", "double"), ("missingValue", "-99.0"), ("units", "W/m^2/nm"))
    ).getOrElse(???)
    val tuple = Tuple.fromSeq(Seq[DataType](irradiance, uncertainty)).getOrElse(???)
    val wavelength = Scalar.fromMetadata(
      Metadata(("id", "wavelength"), ("type", "double"), ("missingValue", "-99.0"), ("units", "nm"))
    ).getOrElse(???)
    val inner = Function.from(
      id"spectrum",
      wavelength,
      tuple
    ).getOrElse(???)
    val time = Time.fromMetadata(Metadata(("id", "time"), ("type", "double"), ("units", "days since 1610-01-01"))).getOrElse(???)
    val dataset = new MemoizedDataset(
      Metadata(id"nrl2_ssi_P1Y"),
      Function.from(
        time,
        inner
      ).getOrElse(???),
      null
    )
    assertEquals(
      fromDataset(dataset).asString,
      """Attributes {
        |  samples {
        |    time {
        |      String type "double";
        |      String units "days since 1610-01-01";
        |    }
        |    spectrum {
        |      wavelength {
        |        String type "double";
        |        String missingValue "-99.0";
        |        String units "nm";
        |      }
        |      unknown {
        |        irradiance {
        |          String type "double";
        |          String missingValue "-99.0";
        |          String units "W/m^2/nm";
        |        }
        |        uncertainty {
        |          String type "double";
        |          String missingValue "-99.0";
        |          String units "W/m^2/nm";
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replace("\r", "")
    )
  }
}