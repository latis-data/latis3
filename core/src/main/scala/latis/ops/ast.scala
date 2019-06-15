package latis.ops

import scala.language.implicitConversions

import cats._
import cats.implicits._
import higherkindness.droste._
import higherkindness.droste.data.Fix

import latis.data._
import latis.metadata.Metadata
import latis.model._

sealed trait SOp
final case object Eq  extends SOp
final case object Gt  extends SOp
final case object Gte extends SOp
final case object Lt  extends SOp
final case object Lte extends SOp

object SOp {
  implicit val showSOp: Show[SOp] = Show.show {
    case Eq  => "="
    case Gt  => ">"
    case Gte => ">="
    case Lt  => "<"
    case Lte => "<="
  }
}

// TODO: Add VariableF?
sealed trait OpF[F]
final case class DatasetF[F](ds: Dataset)                            extends OpF[F]
final case class JoinF[F](ds1: F, ds2: F)                            extends OpF[F]
final case class ProjectF[F](ds: F, variables: List[String])         extends OpF[F]
final case class SelectF[F](ds: F, sop: SOp, vr: String, vl: String) extends OpF[F]

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
      case SelectF(a, s, vr, vl)  => SelectF(f(a), s, vr, vl)
    }
  }
}

object Op {
  type Op = Fix[OpF]

  def renderSelect(e: SelectF[_]): String = e match {
    case SelectF(_, sop, vr, vl) =>
      show"$vr $sop $vl"
  }
}

object ast {
  import Op._

  def dataset(ds: Dataset): Op =
    Fix(DatasetF(ds))

  implicit def dataset2op(ds: Dataset): Op =
    dataset(ds)

  def join(ds1: Op, ds2: Op): Op =
    Fix(JoinF(ds1, ds2))

  def project(ds: Op, variables: List[String]): Op =
    Fix(ProjectF(ds, variables))

  def select(ds: Op, sop: SOp, vr: String, vl: String): Op =
    Fix(SelectF(ds, sop, vr, vl))

  // type EvalError[A] = Either[String, A]
  val evalAlg: Algebra[OpF, Dataset] =
    Algebra {
      case DatasetF(ds) => ds
      case JoinF(Dataset(m, t, d1), Dataset(_, _, d2)) =>
        val f = SampledFunction(d1.streamSamples ++ d2.streamSamples)
        Dataset(m, t, f)
      case s @ SelectF(ds, sop, vr, vl) =>
        Selection(renderSelect(s))(ds)
      case ProjectF(ds, vars) =>
        Projection(vars: _*)(ds)
    }

  val eval: Op => Dataset =
    scheme.cata(evalAlg)

  // NOTE: By using TransM we could capture cases where the selections
  // lead to a contradiction. We could even split this simplification
  // up into two: one that may fail (when the comparisons may not
  // overlap) and one that always works (when the comparisions will
  // always overlap).
  val simplifySelections: Trans[OpF, OpF, Op] =
    Trans {
      // x >= a, x >= b ===> x >= max(a, b)
      case SelectF(Fix(SelectF(r, Gte, varA, a)), Gte, varB, b)
          if varA == varB =>
        val vl = if (a.toInt >= b.toInt) a else b
        SelectF(r, Gte, varA, vl)
      // x <= a, x <= b ===> x <= min(a, b)
      case SelectF(Fix(SelectF(r, Lte, varA, a)), Lte, varB, b)
          if varA == varB =>
        val vl = if (a.toInt <= b.toInt) a else b
        SelectF(r, Lte, varA, vl)
      // x = a, x = b, a = b ===> x = a
      case SelectF(Fix(SelectF(r, Eq, varA, a)), Eq, varB, b)
          if varA == varB && a.toInt == b.toInt =>
        SelectF(r, Eq, varA, a)
      // x >= a, x <= b, a = b ===> x = a
      case SelectF(Fix(SelectF(r, Gte, varA, a)), Lte, varB, b)
          if varA == varB && a.toInt == b.toInt =>
        SelectF(r, Eq, varA, a)
      // x <= a, x >= b, a = b ===> x <= a
      case SelectF(Fix(SelectF(r, Lte, varA, a)), Gte, varB, b)
          if varA == varB && a.toInt == b.toInt =>
        SelectF(r, Lte, varA, a)
      // x = a, x >= b, a >= b ===> x = a
      case SelectF(Fix(SelectF(r, Eq, varA, a)), Gte, varB, b)
          if varA == varB && a.toInt >= b.toInt =>
        SelectF(r, Eq, varA, a)
      // x >= a, x = b, b >= a ===> x = b
      case SelectF(Fix(SelectF(r, Gte, varA, a)), Eq, varB, b)
          if varA == varB && b.toInt >= a.toInt =>
        SelectF(r, Eq, varA, b)
      // x = a, x <= b, a <= b ===> x = a
      case SelectF(Fix(SelectF(r, Eq, varA, a)), Lte, varB, b)
          if varA == varB && a.toInt <= b.toInt =>
        SelectF(r, Eq, varA, a)
      // x <= a, x = b, b <= a ===> x = b
      case SelectF(Fix(SelectF(r, Lte, varA, a)), Eq, varB, b)
          if varA == varB && b.toInt <= a.toInt =>
        SelectF(r, Eq, varA, b)
      // x = a, x > b, a > b ===> x = a
      case SelectF(Fix(SelectF(r, Eq, varA, a)), Gt, varB, b)
          if varA == varB && a.toInt > b.toInt =>
        SelectF(r, Eq, varA, a)
      // x > a, x = b, b > a ===> x = b
      case SelectF(Fix(SelectF(r, Gt, varA, a)), Eq, varB, b)
          if varA == varB && b.toInt > a.toInt =>
        SelectF(r, Eq, varA, b)
      // x = a, x < b, a < b ===> x = a
      case SelectF(Fix(SelectF(r, Eq, varA, a)), Lt, varB, b)
          if varA == varB && a.toInt < b.toInt =>
        SelectF(r, Eq, varA, a)
      // x < a, x = b, b < a ===> x = b
      case SelectF(Fix(SelectF(r, Lt, varA, a)), Eq, varB, b)
          if varA == varB && b.toInt < a.toInt =>
        SelectF(r, Eq, varA, b)
      // x >= a, x > b, b > a ===> x > b
      case SelectF(Fix(SelectF(r, Gte, varA, a)), Gt, varB, b)
          if varA == varB && b.toInt > a.toInt =>
        SelectF(r, Gt, varA, b)
      // x > a, x >= b, a > b ===> x > a
      case SelectF(Fix(SelectF(r, Gt, varA, a)), Gte, varB, b)
          if varA == varB && a.toInt > b.toInt =>
        SelectF(r, Gt, varA, a)
        // x <= a, x < b, b < a ===> x < b
      case SelectF(Fix(SelectF(r, Lte, varA, a)), Lt, varB, b)
          if varA == varB && b.toInt < a.toInt =>
        SelectF(r, Lt, varA, b)
      // x < a, x <= b, a < b ===> x < a
      case SelectF(Fix(SelectF(r, Lt, varA, a)), Lte, varB, b)
          if varA == varB && a.toInt < b.toInt =>
        SelectF(r, Lt, varA, a)
      case x => x
    }

  val simplify: Op => Op =
    scheme.cata(simplifySelections.algebra)

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
