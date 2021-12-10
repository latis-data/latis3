package latis.input

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.effect.Resource
import fs2.io.file.Files
import fs2.Stream
import org.scalatest.Inside.inside
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import latis.data._
import latis.dataset.Dataset
import latis.dsl._
import latis.ops._
import latis.output.NetcdfEncoder

class NetcdfWrapperSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val dataset: Dataset = DatasetGenerator("(x, y) -> (a, b)")
  /*
    (x, y) -> (a, b)
    (0, 0) -> (0, 1)
    (0, 1) -> (2, 3)
    (0, 2) -> (4, 5)
    (1, 0) -> (6, 7)
    (1, 1) -> (8, 9)
    (1, 2) -> (10, 11)
   */

  private val config = new NetcdfAdapter.Config("chunkSize" -> "1")

  private var ncWapper: NetcdfWrapper = _
  private var finalizer: IO[Unit] = _

  override def beforeAll(): Unit = {
    (for {
      path <- Files[IO].tempFile(None, "netcdf_test", ".nc", None)
      enc   = NetcdfEncoder(path.toNioPath.toFile)
      file <- Resource.eval(enc.encode(dataset).compile.toList.map(_.head))
      nc   <- NetcdfWrapper.open(file.toURI, dataset.model, config)
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
      assert(s.toString == "0:1:1,0:2:1")
    }.unsafeRunSync()
  }

  test("head section") {
    val ops = List(Head())
    ncWapper.makeSection(ops).map { s =>
      assert(s.toString == "0:0:1,0:0:1")
    }.unsafeRunSync()
  }

  test("stride section") {
    val ops = List(Stride(List(2,2)))
    ncWapper.makeSection(ops).map { s =>
      assert(s.toString == "0:1:2,0:2:2")
    }.unsafeRunSync()
  }

  test("chunk") {
    (for {
      sec <- Stream.eval(ncWapper.makeSection(List()))
      ss  <- ncWapper.chunkSection(sec)
    } yield ss).compile.toList.map { list =>
      assert(list.length == 2)
    }.unsafeRunSync()
  }

  test("first sample") {
    (for {
      sec <- ncWapper.makeSection(List(Head()))
      s   <- ncWapper.streamSamples(sec).compile.toList.map(_.head)
    } yield s).map { s => inside(s) {
      case Sample(DomainData(Integer(x), Integer(y)), RangeData(Integer(a), Integer(b))) =>
        assert(x == 0)
        assert(y == 0)
        assert(a == 0)
        assert(b == 1)
    }}.unsafeRunSync()
  }

  //TODO: Memoize inner domain variables to avoid re-reading data.
  ignore("memoize") {
    val as = Stream.evalSeq(IO.println("Making As") >> IO(List(1,2,3)))
    val bs = Stream.evalSeq(IO.println("Making Bs") >> IO(List(1,2,3)))

    bs.compile.toList.flatMap { bl =>
      val s = for {
        a <- as
        b <- Stream.emits(bl) //evalSeq(IO(bl))
      } yield (a, b)
      s.map(println).compile.drain
    }.unsafeRunSync()
  }
}
