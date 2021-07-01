package latis.model

import scala.collection.mutable.ArrayBuffer

import cats.syntax.all._

import latis.data._
import latis.metadata._
import latis.util.DefaultDatumOrdering
import latis.util.Identifier
import latis.util.LatisException
import latis.util.ReflectionUtils

/**
 * Define the algebraic data type for the LaTiS implementation of the
 * Functional Data Model.
 */
sealed trait DataType extends MetadataLike with Serializable {

  /** Recursively apply f to this DataType. */
  def map(f: DataType => DataType): DataType = this match {
    case s: Scalar => f(s)
    case t @ Tuple(es @ _*) => f(Tuple(t.metadata, es.map(f)))
    case func @ Function(d, r) => f(Function(func.metadata, d.map(f), r.map(f)))
  }

  /**
   * Return a List of Scalars in the (depth-first) order
   * that they appear in the model.
   */
  def getScalars: List[Scalar] = {
    def go(dt: DataType): List[Scalar] = dt match {
      case s: Scalar => List(s)
      case Tuple(es @ _*) => es.flatMap(go).toList
      case Function(d, r) => go(d) ++ go(r)
    }
    go(this)
  }

  /**
   * Get the DataType of a variable by its identifier.
   */
  def getVariable(id: Identifier): Option[DataType] =
    getScalars.find(_.id.contains(id))
    //TODO: support finding any variable type

  /**
   * Find the DataType of a variable by its identifier or aliases.
   */
  def findVariable(id: Identifier): Option[DataType] =
    findAllVariables(id).headOption
    //TODO: support aliases

  /**
   * Find all Variables within this Variable by the given name.
   */
  def findAllVariables(id: Identifier): Seq[DataType] = {
    id.asString.split('.') match {
      case Array(_) =>
        val vbuf = ArrayBuffer[DataType]()
        if (this.id.contains(id)) vbuf += this //TODO: use hasName to cover aliases?
        this match {
          case _: Scalar =>
          case Tuple(es @ _*) =>
            vbuf ++= es.flatMap(_.findAllVariables(id))
          case Function(d, r) => 
            vbuf ++= d.findAllVariables(id)
            vbuf ++= r.findAllVariables(id)
        }
        vbuf.toSeq
      case Array(n1, n2 @ _*) =>
        //Note, first .get is safe because n1 came from a valid Identifier.
        //TODO: Second .get is safe now, but wouldn't be if dots ('.') were removed from Identifier
        findAllVariables(Identifier.fromString(n1).get)
          .flatMap(_.findAllVariables(Identifier.fromString(n2.mkString(".")).get))
    }
  }

  /**
   * Return the function arity of this DataType.
   * For Function, this is the number of top level types (non-flattened)
   * in the domain.
   * For Scalar and Tuple, there is no domain so the arity is 0.
   */
  def arity: Int = this match {
    case Function(domain, _) =>
      domain match {
        case _: Scalar   => 1
        case t: Tuple    => t.elements.length
        case _: Function => ??? //deal with Function in the domain, or disallow it
      }
    case _ => 0
  }

  // Used by Rename Operation, Pivot Operation, and this.flatten
  def rename(id: Identifier): DataType = this match {
    //TODO: add old name to alias?
    case _: Scalar      => Scalar(metadata   + ("id" -> id.asString))
    case Tuple(es @ _*) => Tuple(metadata    + ("id" -> id.asString), es)
    case Function(d, r) => Function(metadata + ("id" -> id.asString), d, r)
  }

  /**
   * Return this DataType with all nested Tuples flattened to a single Tuple.
   * A Scalar will remain a Scalar.
   * This form is consistent with Samples which don't preserve nested Tuples.
   * Flattened Tuples retain the ID of the outermost Tuple.
   */
  def flatten: DataType = {
    var tupIds = ""
    // Recursive helper function that uses an accumulator (acc)
    // to accumulate types while in the context of a Tuple
    // while the recursion results build up the final type.
    def go(dt: DataType, acc: Seq[DataType]): Seq[DataType] = dt match {
      //prepend Tuple ID(s) with dot(s) and drop leading dot(s)
      case s: Scalar =>
        val sId = s.id.fold("")(_.asString)
        val namespacedId = s"$tupIds.$sId".replaceFirst("^\\.+", "")
        acc :+ s.rename(
          Identifier.fromString(namespacedId).getOrElse {
            throw LatisException(s"Found invalid identifier: $namespacedId")
          }
        )
      case Function(d, r) => acc :+ Function(d.flatten, r.flatten)
      //build up a dot-separated String of Tuple IDs, including empty IDs that stand in for anonymous Tuples
      case t @ Tuple(es @ _*) =>
        val tId = t.id.fold("")(_.asString)
        if (tupIds.isEmpty && tId.nonEmpty) tupIds = tId else tupIds += s".$tId"
        es.flatMap(e => acc ++ go(e, Seq()))
    }

    val types = go(this, Seq())
    types.length match {
      case 1 => types.head
      case _ => 
        if (tupIds.split('.').isEmpty) Tuple(types)
        else if (tupIds.split('.').head.isEmpty) Tuple(types)
        else {
          val tupId = tupIds.split('.').head
          val id = Identifier.fromString(tupId).getOrElse {
            throw LatisException(s"Found invalid identifier: $tupId")
          }
          Tuple(Metadata(id), types)
        }
    }
  }

  /**
   * Return the path within this DataType to a given variable ID
   * as a sequence of SamplePositions.
   * Note that length of the path reflects the number of nested Functions.
   * When searching a Tuple's ID, the path to the first Scalar in the Tuple is returned.
   * Note that Index variables will be ignored since they are not represented in Samples.
   */
  def getPath(id: Identifier): Option[SamplePath] = {
    // Recursive function to try paths until it finds a match
    def go(dt: DataType, id: String, currentPath: SamplePath): Option[SamplePath] = {
      //TODO: use hasName to cover aliases?
      //searching fully qualified ID with namespace
      val dtId = dt.id.fold("")(_.asString)
      if (dtId == id || dtId.split('.').contains(id)) { //found it
        if (dt.isInstanceOf[Index]) None //Index variables have no data in Sample
        else if (currentPath.isEmpty) Some(List(RangePosition(0))) //constant
        else Some(currentPath)
      } else {
        dt match { //recurse
          case _: Scalar => None //dead end
          case _: Tuple => ??? //TODO: allow constant Tuple
          case Function(dtype, rtype) =>
            val ds = dtype match {
              case s: Scalar      => Seq(s)
              case Tuple(vs @ _*) => vs.filterNot(_.isInstanceOf[Index])
            }
            val rs = rtype match {
              case Tuple(vs @ _*) => vs
              case _              => Seq(rtype)
            }

            val d = ds.indices.iterator.map { i => // lazy Iterator allows us to short-circuit
              go(ds(i), id, currentPath :+ DomainPosition(i))
            }
            val r = rs.indices.iterator.map { i => // lazy Iterator allows us to short-circuit
              go(rs(i), id, currentPath :+ RangePosition(i))
            }

            (d ++ r).collectFirst { case Some(p) => p } //short-circuit here, take first Some
        }
      }
    }

    //Note "flatten" so we don't have to deal with nested Tuples
    //  Namespace should be preserved with dot (".") notation.
    go(this.flatten, id.asString, List.empty)
  }

  /** Makes fill data for this data type. */
  def fillData: Data = Data.fromSeq(getFillData(this, Seq.empty))

  /** Recursive helper function */
  private def getFillData(dt: DataType, acc: Seq[Data]): Seq[Data] = dt match {
    case s: Scalar => acc :+
      s.metadata.getProperty("fillValue").orElse {
        s.metadata.getProperty("missingValue")
      }.map { fv =>
        s.parseValue(fv).fold(throw _, identity)  //TODO: validate earlier
      }.getOrElse(NullData)
    case Tuple(es @ _*) => es.flatMap(getFillData(_, acc))
    case _: Function => acc :+ NullData
  }
}

//-- Scalar -----------------------------------------------------------------//

/**
 * The Scalar type represents a single atomic variable.
 */
class Scalar(val metadata: Metadata) extends DataType {
  //TODO: type safe units,...
  //TODO: use ScalarOps to clean up this file? in package?
  //TODO: construct with type ..., use smart constructor to build from Metadata,
  //      or type safe Metadata

  // Note, this will fail eagerly if constructing a Scalar
  // with an invalid value type.
  val valueType: ValueType = metadata.getProperty("type").map {
    ValueType.fromName(_) match {
      case Right(v) => v
      case Left(e)  => throw e
    }
  }.getOrElse {
    val msg = s"No value type defined for Scalar: $id"
    throw new RuntimeException(msg)
  }

  /** Specifies if fillData can be used to replace invalid data. */
  def isFillable: Boolean = metadata.getProperty("fillValue").nonEmpty

  /**
   * Converts a string value into the appropriate Datum type for this Scalar.
   *
   * If the value fails to parse and a fillValue is explicitly defined in the metadata,
   * the resulting Datum will encode that fill value. Because this returns a Datum,
   * NullData ("null") may not be used for fillValue metadata.
   */
  def parseValue(value: String): Either[LatisException, Datum] =
    valueType.parseValue(value).recoverWith { ex =>
      if (isFillable) fillData match {
        // Make sure fill value is not NullData
        case fv: Datum => fv.asRight
        case fv => LatisException(s"Invalid fillValue: $fv").asLeft //TODO: validate earlier
      } else {
        ex.asLeft
      }
    }

  /**
   * Returns a string representation of the given Data.
   * This should be used to get a string representation of a Datum
   * instead of Datum.asString so properties of the Scalar, such as
   * precision, can be applied to an otherwise agnostic data value.
   */
  def formatValue(data: Data): String =
    //TODO: disallow construction of non-real Scalar with precision metadata
    //TODO: validate construction with precision as int >= 0
    (data, metadata.getProperty("precision")) match {
      case (Real(d), Some(p)) => (s"%.${p}f").format(d)
      case (d: Datum, _)      => d.asString
      case (NullData, _)      => "null"
      case _ => "error" //Tuple and Function data should not show up here
    }

  /**
   * Converts a string value into the appropriate type and units
   * for this Scalar.
   */
  def convertValue(value: String): Either[Exception, Datum] =
    parseValue(value)
  //TODO: support units, e.g. "1.2 meters"

  /**
   * Defines a PartialOrdering for Datums of the type described by this Scalar.
   * The "order" metadata property can be set to "asc" (default) or "des" for
   * descending.
   */
  def ordering: PartialOrdering[Datum] =
    //TODO: support enumerated values
    metadata.getProperty("order", "asc").toLowerCase.take(3) match {
      case "asc" => DefaultDatumOrdering
      case "des" => DefaultDatumOrdering.reverse
      case s => throw LatisException(s"Invalid order: $s") //TODO: validate sooner
    }

  override def toString: String = id.fold("")(_.asString) //TODO: do we want a different default than ""?
}

object Scalar {
  //TODO enforce id or make uid

  /**
   * Construct a Scalar with the given Metadata.
   * If a class is defined, construct it dynamically.
   */
  def apply(md: Metadata): Scalar = md.getProperty("class") match {
    case Some(clss) => Either.catchNonFatal {
      ReflectionUtils.callMethodOnCompanionObject(
        clss, "apply", md
      ).asInstanceOf[Scalar]
    }.fold(throw _, identity)
    case None => new Scalar(md)
  }
}

//-- Tuple ------------------------------------------------------------------//

/**
 * A Tuple type represents a group of variables of other DataTypes.
 */
class Tuple(val metadata: Metadata, val elements: List[DataType]) extends DataType {

  override def toString: String = elements.mkString("(", ", ", ")")

}

object Tuple {
  //TODO enforce id or make uid

  def apply(metadata: Metadata, es: Seq[DataType]): Tuple =
    new Tuple(metadata, es.toList)

  /**
   * Construct a Tuple from Metadata and a comma separated list
   * of data types.
   */
  def apply(metadata: Metadata, e0: DataType, es: DataType*): Tuple =
    Tuple(metadata, e0 +: es)

  /**
   * Construct a Tuple with default Metadata.
   */
  def apply(e0: DataType, es: DataType*): Tuple =
    Tuple(Metadata(), e0 +: es)

  def apply(es: Seq[DataType]): Tuple =
    Tuple(Metadata(), es)

  /**
   * Extract elements of a Tuple with a comma separated list
   * as opposed to a single Seq.
   */
  def unapplySeq(tuple: Tuple): Option[Seq[DataType]] =
    Option(tuple.elements)
  //TODO: better to return List via unapply?
}

//-- Function ---------------------------------------------------------------//

/**
 * A Function type represents a functional mapping
 * from the domain type to the range type.
 */
class Function(val metadata: Metadata, val domain: DataType, val range: DataType) extends DataType {

  override def toString: String = s"$domain -> $range"
}

object Function {
  //TODO enforce id or make uid

  /**
   * Construct a Function from metadata and the domain and range types.
   */
  def apply(metadata: Metadata, domain: DataType, range: DataType): Function =
    new Function(metadata, domain, range)

  /**
   * Construct a Function with default Metadata.
   */
  def apply(domain: DataType, range: DataType): Function =
    Function(Metadata(), domain, range)

  /**
   * Construct a Function from a Seq of domain variables
   * and a Seq of range variables. This will make a wrapping
   * Tuple only if the Seq has more that one element.
   */
  //TODO: Use Index if one is empty, error if both are empty?
  def apply(ds: Seq[DataType], rs: Seq[DataType]): Function = {
    val domain = ds.length match {
      case 0 => ??? //TODO: Index
      case 1 => ds.head
      case _ => Tuple(ds)
    }
    val range = rs.length match {
      case 0 => ??? //TODO: no range, make domain a function of Index
      case 1 => rs.head
      case _ => Tuple(rs)
    }

    Function(domain, range)
  }

  /**
   * Extract the domain and range types from a Function as a pair.
   */
  def unapply(f: Function): Option[(DataType, DataType)] =
    Option((f.domain, f.range))

}
