package latis.ops

import cats.effect.unsafe.implicits.global
import org.scalatest.Inside._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import latis.data._
import latis.dataset.Dataset
import latis.dataset.MemoizedDataset
import latis.dsl._
import latis.model._

class CurrySuite extends AnyFunSuite {

  test("Curry a 2D dataset") {
    val curriedDs = mock2d.withOperation(Curry()).unsafeForce()
    val samples = curriedDs.samples.compile.toList.unsafeRunSync()

    inside(samples.head) { case Sample(DomainData(Number(t)), RangeData(f)) =>
      assert(t == 1)

      inside(f) { case MemoizedFunction(samples) =>
        inside(samples) { case Vector(s1, s2) =>
          inside(s1) { case Sample(DomainData(Text(d)), RangeData(Number(r1), Number(r2))) =>
            assert(d == "Fe")
            assert(r1 == 1.1)
            assert(r2 == 0.1)
          }

          inside(s2) { case Sample(DomainData(Text(d)), RangeData(Number(r1), Number(r2))) =>
            assert(d == "Mg")
            assert(r1 == 1.2)
            assert(r2 == 0.2)
          }
        }
      }
    }

    assert(curriedDs.model.toString == "_1 -> _2 -> (a, b)")
    inside(curriedDs.model) { case Function(domain, range) =>
      domain shouldBe a[Scalar]
      assert(domain.id.get.asString == "_1")
      range shouldBe a[Function]

      inside(range) { case Function(d, r) =>
        d shouldBe a[Scalar]
        assert(d.id.get.asString == "_2")
        r shouldBe a[Tuple]

        inside(r) { case Tuple(s1, s2) =>
          s1 shouldBe a[Scalar]
          assert(s1.id.get.asString == "a")
          s2 shouldBe a[Scalar]
          assert(s2.id.get.asString == "b")
        }
      }
    }
  }

  test("Curry a 2D dataset to arity 2 (no change)") {
    val curriedDs = mock2d.withOperation(Curry(2)).unsafeForce()
    val samples = curriedDs.samples.compile.toList.unsafeRunSync()

    inside(samples.head) { case Sample(DomainData(Number(d1), Text(d2)), RangeData(Number(r1), Number(r2))) =>
      assert(d1 == 1)
      assert(d2 == "Fe")
      assert(r1 == 1.1)
      assert(r2 == 0.1)
    }

    assert(curriedDs.model.toString == "(_1, _2) -> (a, b)")
    inside(curriedDs.model) { case Function(domain, range) =>
      domain shouldBe a [Tuple]
      range shouldBe a [Tuple]

      inside(domain) { case Tuple(d1, d2) =>
        d1 shouldBe a [Scalar]
        assert(d1.id.get.asString == "_1")
        d2 shouldBe a [Scalar]
        assert(d2.id.get.asString == "_2")
      }

      inside(range) { case Tuple(r1, r2) =>
        r1 shouldBe a [Scalar]
        assert(r1.id.get.asString == "a")
        r2 shouldBe a [Scalar]
        assert(r2.id.get.asString == "b")
      }
    }
  }

  test("Curry a 3D dataset to arity 2") {
    // should be "(x, y) -> z -> flux" after curry
    val curriedDs = mock3d.withOperation(Curry(2)).unsafeForce()
    val samples = curriedDs.samples.compile.toList.unsafeRunSync()

    inside(samples.head) { case Sample(DomainData(Integer(d1), Integer(d2)), RangeData(f)) =>
      assert(d1 == 1)
      assert(d2 == 1)

      inside(f) { case MemoizedFunction(samples) =>
        inside(samples) { case Vector(s1, s2) =>
          inside(s1) { case Sample(DomainData(Integer(d)), RangeData(Number(r))) =>
            assert(d == 1)
            assert(r == 10.0)
          }

          inside(s2) { case Sample(DomainData(Integer(d)), RangeData(Number(r))) =>
            assert(d == 2)
            assert(r == 20.0)
          }
        }
      }
    }

    assert(curriedDs.model.toString == "(_1, _2) -> _3 -> a")
    inside(curriedDs.model) { case Function(domain, range) =>
      domain shouldBe a [Tuple]
      range shouldBe a [Function]

      inside(domain) { case Tuple(d1, d2) =>
        d1 shouldBe a [Scalar]
        assert(d1.id.get.asString == "_1")
        d2 shouldBe a [Scalar]
        assert(d2.id.get.asString == "_2")
      }

      inside(range) { case Function(d, r) =>
        d shouldBe a [Scalar]
        assert(d.id.get.asString == "_3")
        r shouldBe a [Scalar]
        assert(r.id.get.asString == "a")
      }
    }
  }

  // (_1, _2) -> (a, b)
  private val mock2d: Dataset =
    DatasetGenerator.generate2DDataset(
      Seq(1,2,3),
      Seq("Fe", "Mg"),
      Seq(Seq(1.1, 1.2, 2.1, 2.2, 3.1, 3.2)),
      Seq(Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6)))

  // (_1, _2, _3) -> a
  private val mock3d: MemoizedDataset =
    DatasetGenerator.generate3DDataset(
      Seq(1,2),
      Seq(1,2),
      Seq(1,2),
      Seq(Seq(Seq(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0))))
}
