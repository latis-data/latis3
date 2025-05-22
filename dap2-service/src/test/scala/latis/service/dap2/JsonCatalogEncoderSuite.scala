package latis.service.dap2

import munit.CatsEffectSuite

import latis.catalog.Catalog
import latis.dataset.MemoizedDataset
import latis.metadata.Metadata
import latis.util.Identifier.*

class JsonCatalogEncoderSuite extends CatsEffectSuite {

  private lazy val ds0 = new MemoizedDataset(Metadata("id"->"ds0", "title"->"Dataset 0"), null, null)
  private lazy val ds1 = new MemoizedDataset(Metadata("id"->"ds1", "title"->"Dataset 1"), null, null)
  private lazy val ds2 = new MemoizedDataset(Metadata("id"->"ds2", "title"->"Dataset 2"), null, null)

  private lazy val catalog: Catalog = {
    Catalog(ds0)
      .addCatalog(id"cat2", Catalog(ds2))
      .addCatalog(id"cat1", Catalog(ds1))
  }

  test("json catalog with ordering") {
    val expected =
      """{
        |  "catalog" : [
        |    {
        |      "identifier" : "cat1",
        |      "dataset" : [
        |        {
        |          "identifier" : "ds1",
        |          "title" : "Dataset 1"
        |        }
        |      ]
        |    },
        |    {
        |      "identifier" : "cat2",
        |      "dataset" : [
        |        {
        |          "identifier" : "ds2",
        |          "title" : "Dataset 2"
        |        }
        |      ]
        |    }
        |  ],
        |  "dataset" : [
        |    {
        |      "identifier" : "ds0",
        |      "title" : "Dataset 0"
        |    }
        |  ]
        |}""".stripMargin
    JsonCatalogEncoder.encode(catalog).map { json =>
      assertEquals(json.toString, expected)
    }
  }
  
  test("dataset ordering") {
    val cat = Catalog(ds1, ds0)
    JsonCatalogEncoder.encode(cat).map { json =>
      json.hcursor.downField("dataset").downArray.downField("identifier").as[String].map { id =>
        assertEquals(id, "ds0")
      }
    }
  }

}
