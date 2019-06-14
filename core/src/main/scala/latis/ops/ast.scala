package latis.ops

import scala.language.implicitConversions

import cats._
import higherkindness.droste._
import higherkindness.droste.data.Fix

import latis.data._
import latis.metadata.Metadata
import latis.model._

// sealed trait SExpr
// final case class Gte(variable: String, value: String) extends SExpr
// final case class Lt(variable: String, value: String)  extends SExpr
//
// object SExpr {
//   def >=(variable: String, value: String): SExpr =
//     Gte(variable, value)
//
//   def <(variable: String, value: String): SExpr =
//     Lt(variable, value)
// }

sealed trait OpF[F]
final case class DatasetF[F](ds: Dataset)                    extends OpF[F]
final case class JoinF[F](ds1: F, ds2: F)                    extends OpF[F]
final case class ProjectF[F](ds: F, variables: List[String]) extends OpF[F]
final case class SelectF[F](ds: F, sexpr: String)            extends OpF[F]

object OpF {
  // implicit val traverseOpF: Traverse[OpF] = new Traverse[OpF] {
  //   def traverse[G[_]: Applicative, A, B](fa: OpF[A])(f: A => G[B]): G[OpF[B]] =
  //     fa match {
  //       case DatasetF(ds)    => (DatasetF(ds): OpF[B]).pure[G]
  //       case JoinF(ds1, ds2) => (f(ds1), f(ds2)).mapN(JoinF(_, _))
  //       case ProjectF(ds, v) => f(ds).map(ProjectF(_, v))
  //       case SelectF(ds, s)  => f(ds).map(SelectF(_, s))
  //     }
  // }

  implicit val functorOpF: Functor[OpF] = new Functor[OpF] {
    def map[A, B](fa: OpF[A])(f: A => B): OpF[B] = fa match {
      case DatasetF(ds)   => DatasetF(ds)
      case JoinF(a, b)    => JoinF(f(a), f(b))
      case ProjectF(a, v) => ProjectF(f(a), v)
      case SelectF(a, s)  => SelectF(f(a), s)
    }
  }
}

object ast {
  type Op = Fix[OpF]

  def dataset(ds: Dataset): Op =
    Fix(DatasetF(ds))

  implicit def dataset2op(ds: Dataset): Op =
    dataset(ds)

  def join(ds1: Op, ds2: Op): Op =
    Fix(JoinF(ds1, ds2))

  def project(ds: Op, variables: List[String]): Op =
    Fix(ProjectF(ds, variables))

  def select(ds: Op, sexpr: String): Op =
    Fix(SelectF(ds, sexpr))

  // type EvalError[A] = Either[String, A]
  val evalAlg: Algebra[OpF, Dataset] =
    Algebra {
      case DatasetF(ds) => ds
      case JoinF(Dataset(m, t, d1), Dataset(_, _, d2)) =>
        val f = SampledFunction(d1.streamSamples ++ d2.streamSamples)
        Dataset(m, t, f)
      case SelectF(ds, expr) =>
        Selection(expr)(ds)
      case ProjectF(ds, vars) =>
        Projection(vars: _*)(ds)
    }

  val eval: Op => Dataset =
    scheme.cata(evalAlg)

  val ds1: Dataset = Dataset(
    Metadata("ds1"),
    Function(
      Scalar(Metadata("id" -> "time", "type" -> "int")),
      Tuple(
        Scalar(Metadata("id" -> "val1", "type" -> "int")),
        Scalar(Metadata("id" -> "val2", "type" -> "int"))
      )
    ),
    SampledFunction(
      Sample(Vector(1), Vector(1, 2)),
      Sample(Vector(2), Vector(2, 3)),
      Sample(Vector(3), Vector(3, 4))
    )
  )

  val ds2: Dataset = Dataset(
    Metadata("ds2"),
    Function(
      Scalar(Metadata("id" -> "time", "type" -> "int")),
      Tuple(
        Scalar(Metadata("id" -> "val1", "type" -> "int")),
        Scalar(Metadata("id" -> "val2", "type" -> "int"))
      )
    ),
    SampledFunction(
      Sample(Vector(4), Vector(4, 5)),
      Sample(Vector(5), Vector(5, 6)),
      Sample(Vector(6), Vector(6, 7))
    )
  )
}
