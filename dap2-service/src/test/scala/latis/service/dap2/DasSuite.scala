package latis.service.dap2

import cats.data.NonEmptyList
import munit.CatsEffectSuite

import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.model._
import latis.service.dap2.AtomicType._
import latis.service.dap2.Das._
import latis.time.Time
import latis.util.Identifier._

class DasSuite extends CatsEffectSuite {
  val byteAttr = Attribute(id"testByte", Byte, NonEmptyList.one(42:Byte))
  val uintAttr = Attribute(id"testInt", UInt32, NonEmptyList.one(42L))
  val floatAttr = Attribute(id"testFloat", Float32, NonEmptyList.one(13.14159265359f))
  val stringAttr = Attribute(id"testString", String, NonEmptyList.one("str1"))
  val stringsAttr = Attribute(id"testStrings", String, NonEmptyList.fromListUnsafe(List("str1", "str2", "str3")))
  val itemAttrCont = ItemAttributeCont(id"items", List(byteAttr, uintAttr, floatAttr, stringAttr, stringsAttr))
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
      |    }
      |    more_nothing {
      |    }
      |  }
      |}""".stripMargin.replace("\r", "")

  test("correct Attribute toDoc representation") {
    assertEquals(byteAttr.toDoc.render(1), "Byte testByte 42;")
    assertEquals(uintAttr.toDoc.render(1), "UInt32 testInt 42;")
    assertEquals(floatAttr.toDoc.render(1), "Float32 testFloat 13.1416;")
    assertEquals(stringAttr.toDoc.render(1), "String testString \"str1\";")
    assertEquals(stringsAttr.toDoc.render(1), "String testStrings \"str1\", \"str2\", \"str3\";")
  }

  test("correct ItemAttributeCont toDoc representation") {
    assertEquals(ItemAttributeCont(id"empty", List()).toDoc.render(1), "empty {\n}")
    assertEquals(
      itemAttrCont.toDoc.render(1),
      itemAttrStr
    )
  }

  test("correct NestedAttributeCont toDoc representation") {
    assertEquals(NestedAttributeCont(id"empty", List()).toDoc.render(1), "empty {\n}")
    assertEquals(
      nestedAttrCont.toDoc.render(1),
      nestedAttrStr
    )
  }

  test("correct Das asString representation") {
    assertEquals(Das(List()).asString, "Attributes {\n}")
    val das = Das(List(itemAttrCont, nestedAttrCont))
    assertEquals(
      das.asString,
      "Attributes {\n  " + itemAttrStr.replace("\n", "\n  ") + "\n  " +
        nestedAttrStr.replace("\n", "\n  ") + "\n}"
    )
  }

  test("correct fromDatatype Scalar attributes") {
    val basicScalar = Scalar(id"test", StringValueType)
    assertEquals(
      fromDataType(basicScalar).toDoc.render(1),
      "test {\n}"
    )
    val metadataScalar = Scalar.fromMetadata(Metadata(("id","test2"), ("type","string"), ("missingValue", "N/A"), ("units", "YYYY-mm-dd")))
      .getOrElse(???)
    assertEquals(
      fromDataType(metadataScalar).toDoc.render(1),
      """test2 {
        |  String missingValue "N/A";
        |  String units "YYYY-mm-dd";
        |}""".stripMargin.replace("\r", "")
    )
  }

  val s1 = Scalar.fromMetadata(Metadata(("id","s1"), ("type","string"))).getOrElse(???)
  val s2 = Scalar.fromMetadata(Metadata(("id","s2"), ("type","double"), ("missingValue", "-99.0"))).getOrElse(???)
  val t1 = Tuple.fromSeq(id"t1", Seq(s1, s2)).getOrElse(???)
  val s3 = Scalar.fromMetadata(Metadata(("id","s3"), ("type","byte"), ("missingValue", "42"), ("units", "meters"))).getOrElse(???)
  test("correct fromDatatype Tuple attributes") {
    val t2 = Tuple.fromSeq(id"t2", Seq[DataType](t1, s3)).getOrElse(???)
    assertEquals(
      fromDataType(t2).toDoc.render(1),
      """t2 {
        |  t1 {
        |    s1 {
        |    }
        |    s2 {
        |      String missingValue "-99.0";
        |    }
        |  }
        |  s3 {
        |    String missingValue "42";
        |    String units "meters";
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
        |    String missingValue "42";
        |    String units "meters";
        |  }
        |  t1 {
        |    s1 {
        |    }
        |    s2 {
        |      String missingValue "-99.0";
        |    }
        |  }
        |}""".stripMargin.replace("\r", "")
    )
    assertEquals(
      fromDataType(f1, true).toDoc.render(1),
      """samples {
        |  s3 {
        |    String missingValue "42";
        |    String units "meters";
        |  }
        |  t1 {
        |    s1 {
        |    }
        |    s2 {
        |      String missingValue "-99.0";
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
        |      String units "days since 1610-01-01";
        |    }
        |    spectrum {
        |      wavelength {
        |        String missingValue "-99.0";
        |        String units "nm";
        |      }
        |      unknown {
        |        irradiance {
        |          String missingValue "-99.0";
        |          String units "W/m^2/nm";
        |        }
        |        uncertainty {
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
