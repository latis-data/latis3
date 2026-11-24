package latis.ops

import cats.effect.IO
import fs2.Stream
import munit.CatsEffectSuite

import latis.data.*
import latis.dsl.*
import latis.model.*
import latis.util.Identifier.id

class AppendSuite extends CatsEffectSuite {

  // Append will blindly append streams of samples
  test("append functions with overlap and duplicate") {
    val model = ModelParser.unsafeParse("x -> a")
    val samples1: Stream[IO, Sample] = Stream.emits(List(0, 2, 4)).map { n =>
      Sample(DomainData(Data.IntValue(n)), RangeData(Data.IntValue(n)))
    }
    val samples2 = Stream.emits(List(3, 4, 5)).map { n =>
      Sample(DomainData(Data.IntValue(n)), RangeData(Data.IntValue(n)))
    }
    Append().applyToData(
      model, samples1,
      model, samples2
    ).fold(throw _, identity).compile.toList.map { ss =>
      val obs = ss.map {
        case Sample(DomainData(i: Data.IntValue), _) => i.value
      }
      val exp = List(0, 2, 4, 3, 4, 5)
      assertEquals(obs, exp)
    }
  }

  test("append scalars") {
    val model = Scalar(id"test", IntValueType)
    val s1 = Stream.emit(Sample(DomainData(), RangeData(Data.IntValue(1))))
    val s2 = Stream.emit(Sample(DomainData(), RangeData(Data.IntValue(2))))
    Append().applyToData(
      model, s1,
      model, s2
    ).fold(throw _, identity).compile.count.map { n =>
      assertEquals(n, 2L)
    }
  }

  // Scalars or Tuples will be appended as a Function of Index
  test("append scalars model") {
    val model = Scalar(id"test", IntValueType)
    Append().applyToModel(model, model).map {
      case Function(_: Index, _: Scalar) => assert(true)
      case _ => fail("model application failed")
    }
  }

}
