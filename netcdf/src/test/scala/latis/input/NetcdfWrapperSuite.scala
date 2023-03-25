package latis.input

import cats.effect.IO
import cats.effect.Resource
import fs2.io.file.Files
import fs2.Stream
import munit.CatsEffectSuite

import latis.data._
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.dsl._
import latis.metadata.Metadata
import latis.model._
import latis.ops._
import latis.output.NetcdfEncoder
import latis.util.Identifier.IdentifierStringContext

class NetcdfWrapperSuite extends CatsEffectSuite {

  /*
  (x, y) -> (a, b)
  (0, 0) -> (0, 1)
  (0, 1) -> (2, 3)
  (0, 2) -> (4, 5)
  (1, 0) -> (6, 7)
  (1, 1) -> (8, 9)
  (1, 2) -> (10, 11)
  */
  private lazy val dataset: Dataset = {
    val model = (for {
      x <- Scalar.fromMetadata(Metadata(
        "id" -> "x",
        "type" -> "int",
        "cadence" -> "1",
        "coverage" -> "0/1"
      ))
      y <- Scalar.fromMetadata(Metadata(
        "id" -> "y",
        "type" -> "int",
        "cadence" -> "1",
        "coverage" -> "0/2"
      ))
      d <- Tuple.fromElements(x, y)
      r <- Tuple.fromElements(
        Scalar(id"a", IntValueType),
        Scalar(id"b", IntValueType)
      )
      f <- Function.from(d, r)
    } yield f).getOrElse(fail("Bad model"))

    val data = DatasetGenerator.generateData(model)

    new MemoizedDataset(Metadata(id"test"), model, data)
  }

  private val config = new NetcdfAdapter.Config("chunkSize" -> "1")

  private var ncWapper: NetcdfWrapper = _
  private var finalizer: IO[Unit] = _

  //TODO: use ResourceSuiteLocalFixture?
  override def beforeAll(): Unit = {
    (for {
      path <- Files[IO].tempFile(None, "netcdf_test", ".nc", None)
      enc   = NetcdfEncoder(path)
      file <- Resource.eval(enc.encode(dataset).compile.toList.map(_.head))
      nc   <- NetcdfWrapper.open(file.toNioPath.toUri(), dataset.model, config)
    } yield nc).allocated.map { case (nc, io) =>
      ncWapper = nc
      finalizer = io
    }.unsafeRunSync()
  }

  override def afterAll(): Unit = {
    finalizer.unsafeRunSync()
  }


  test("default section") {
    ncWapper.makeSection(List()).map { s =>
      assertEquals(s.toString, "0:1:1,0:2:1")
    }
  }

  test("head section") {
    val ops = List(Head())
    ncWapper.makeSection(ops).map { s =>
      assertEquals(s.toString, "0:0:1,0:0:1")
    }
  }

  test("stride section") {
    val ops = List(Stride(List(2,2)))
    ncWapper.makeSection(ops).map { s =>
      assertEquals(s.toString, "0:0:2,0:2:2")
    }
  }

  test("selection with >") {
    val ops = List(Selection.makeSelection("y > 1").getOrElse(fail("Bad selection")))
    ncWapper.makeSection(ops).map { s =>
      assertEquals(s.toString, "0:1:1,2:2:1")
    }
  }

  test("selection with >=") {
    val ops = List(Selection.makeSelection("y >= 1").getOrElse(fail("Bad selection")))
    ncWapper.makeSection(ops).map { s =>
      assertEquals(s.toString, "0:1:1,1:2:1")
    }
  }

  test("selection with <") {
    val ops = List(Selection.makeSelection("y < 1").getOrElse(fail("Bad selection")))
    ncWapper.makeSection(ops).map { s =>
      assertEquals(s.toString, "0:1:1,0:0:1")
    }
  }

  test("selection with <=") {
    val ops = List(Selection.makeSelection("y <= 1").getOrElse(fail("Bad selection")))
    ncWapper.makeSection(ops).map { s =>
      assertEquals(s.toString, "0:1:1,0:1:1")
    }
  }

  test("two selections") {
    val ops = List(
      Selection.makeSelection("y >= 1").getOrElse(fail("Bad selection")),
      Selection.makeSelection("y < 2").getOrElse(fail("Bad selection"))
    )
    ncWapper.makeSection(ops).map { s =>
      assertEquals(s.toString, "0:1:1,1:1:1")
    }
  }

  test("selection outside bounds") {
    val ops = List(Selection.makeSelection("y > 9").getOrElse(fail("Bad selection")))
    ncWapper.makeSection(ops).map { s =>
      assert(s.isEmpty)
    }
  }

  test("stride then selection with truncation") {
    val ops = List(
      Stride(List(1, 2)),
      Selection.makeSelection("y >= 1").getOrElse(fail("Bad selection"))
    )
    ncWapper.makeSection(ops).map { s =>
      assertEquals(s.toString, "0:1:1,2:2:2")
    }
  }

  test("chunk") {
    (for {
      sec <- Stream.eval(ncWapper.makeSection(List()))
      ss  <- ncWapper.chunkSection(sec)
    } yield ss).compile.toList.map { list =>
      assertEquals(list.length, 2)
    }
  }

  test("first sample") {
    (for {
      sec <- ncWapper.makeSection(List(Head()))
      s   <- ncWapper.streamSamples(sec).compile.toList.map(_.head)
    } yield s).map {
      case Sample(DomainData(Integer(x), Integer(y)), RangeData(Integer(a), Integer(b))) =>
        assertEquals(x, 0L)
        assertEquals(y, 0L)
        assertEquals(a, 0L)
        assertEquals(b, 1L)
      case _ => fail("Invalid Sample")
    }
  }

  //TODO: Memoize inner domain variables to avoid re-reading data.
  test("memoize".ignore) {
    val as = Stream.evalSeq(IO.println("Making As") >> IO(List(1,2,3)))
    val bs = Stream.evalSeq(IO.println("Making Bs") >> IO(List(1,2,3)))

    bs.compile.toList.flatMap { bl =>
      val s = for {
        a <- as
        b <- Stream.emits(bl) //evalSeq(IO(bl))
      } yield (a, b)
      s.map(println).compile.drain
    }
  }
}
