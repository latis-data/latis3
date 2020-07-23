package latis.model

import latis.data._
import latis.metadata._
import latis.util.DefaultDatumOrdering
import latis.util.LatisException

import scala.collection.mutable.ArrayBuffer

/**
 * Define the algebraic data type for the LaTiS implementation of the
 * Functional Data Model.
 */
sealed trait DataType extends MetadataLike with Serializable {

  // Apply f to Scalars only, for now
  def map(f: DataType => DataType): DataType = this match {
    case s: Scalar => f(s)
    case Tuple(es @ _*) => Tuple(es.map(f))
    case Function(d, r) => Function(d.map(f), r.map(f))
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
  def getVariable(id: String): Option[DataType] =
    getScalars.find(_.id == id)
    //TODO: support finding any variable type

  /**
   * Find the DataType of a variable by its identifier or aliases.
   */
  def findVariable(variableName: String): Option[DataType] =
    getVariable(variableName)
    //TODO: support aliases

  /**
   * Find all Variables within this Variable by the given name.
   * TODO: support aliases
   * TODO: consider nested tuple id's like B.x (use this.id.split('.').last?)
   */
  def findAllVariables(variableName: String): Seq[DataType] = {
    variableName.split('.') match {
      case Array(n) => {
        val vbuf = ArrayBuffer[DataType]()
        if (this.id == variableName) vbuf += this //TODO: use hasName to cover aliases?
        this match {
          case _: Scalar =>
          case Tuple(es @ _*) =>
            val matchingVars = es.flatMap(_.findAllVariables(variableName))
            vbuf ++= matchingVars
          case Function(d, r) => 
            vbuf ++= d.findAllVariables(variableName) 
            vbuf ++= r.findAllVariables(variableName)
        }
        vbuf.toSeq
      }
      case Array(n1, n2 @ _*) => {
        findAllVariables(n1).flatMap(_.findAllVariables(n2.mkString(".")))
      }
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

  // Used by Rename Operation
  def rename(name: String): DataType = this match {
    //TODO: add old name to alias?
    case _: Scalar      => Scalar(metadata + ("id"   -> name))
    case Tuple(es @ _*) => Tuple(metadata + ("id"    -> name), es)
    case Function(d, r) => Function(metadata + ("id" -> name), d, r)
  }

  /**
   * Return this DataType with all nested Tuples flattened to a single Tuple.
   * A Scalar will remain a Scalar.
   * This form is consistent with Samples which don't preserve nested Functions.
   */
  def flatten: DataType = {
    var tupName = ""
    // Recursive helper function that uses an accumulator (acc)
    // to accumulate types while in the context of a Tuple
    // while the recursion results build up the final type.
    def go(dt: DataType, acc: Seq[DataType]): Seq[DataType] = dt match {
      case s: Scalar      => acc :+ s
      case t @ Tuple(es @ _*) =>
        if (tupName == "") tupName = t.id //else tupName = s"$tupName.${t.id}" //TODO: reconsider use of var
        es.flatMap(e => e match {
          case _: Scalar if t.id != "" =>
            acc ++ go(e.rename(s"${t.id}.${e.id}"), Seq()) //prepend Tuple ID to Scalar ID with "."
          case _ => acc ++ go(e, Seq()) 
        })
      case Function(d, r) => acc :+ Function(d.flatten, r.flatten)
    }

    val types = go(this, Seq())
    types.length match {
      case 1 => types.head
      case _ => Tuple(Metadata(tupName), types) //TODO: this Tuple needs an ID that's preserved from before the flatten...
    }
  }

  /**
   * Return the path within this DataType to a given variable ID
   * as a sequence of SamplePositions.
   * Note that length of the path reflects the number of nested Functions.
   * TODO: Tuples are improperly handled. There's no option to return a path to a Tuple with a matching ID.
   */
  def getPath(id: String): Option[SamplePath] = {

    // Recursive function to try paths until it finds a match
    def go(dt: DataType, id: String, currentPath: SamplePath): Option[SamplePath] =
      if (id == dt.id) Some(currentPath) //found it  //TODO: use hasName to cover aliases?
      else
        dt match { //recurse
          case _: Scalar => None //dead end
          case Function(dtype, rtype) =>
            val ds = dtype match {
              case s: Scalar      => Seq(s)
              case Tuple(vs @ _*) => vs
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

    go(this.flatten, id, List.empty)
  }

//  /**
//   * Just like getPath except nested Tuples are not flattened.
//   */
//  def getPathWithoutFlattening(id: String): Option[SamplePath] = {
//
//    // Recursive function to try paths until it finds a match
//    def go(dt: DataType, id: String, currentPath: SamplePath): Option[SamplePath] =
//      if (id == dt.id) Some(currentPath) //found it  //TODO: use hasName to cover aliases?
//      else
//        dt match { //recurse
//          case _: Scalar => None //dead end
//          case Function(dtype, rtype) =>
//            val ds = dtype match {
//              case s: Scalar      => Seq(s)
//              case Tuple(vs @ _*) => vs
//            }
//            val rs = rtype match {
//              case Tuple(vs @ _*) => vs
//              case _              => Seq(rtype)
//            }
//
//            val d = ds.indices.iterator.map { i => // lazy Iterator allows us to short-circuit
//              go(ds(i), id, currentPath :+ DomainPosition(i))
//            }
//            val r = rs.indices.iterator.map { i => // lazy Iterator allows us to short-circuit
//              go(rs(i), id, currentPath :+ RangePosition(i))
//            }
//
//            (d ++ r).collectFirst { case Some(p) => p } //short-circuit here, take first Some
//        }
//
//    go(this, id, List.empty)
//  }

  //TODO: beef up
  //TODO: use missingValue in metadata, scalar.parseValue(s)
  def fillValue: Data = Data.fromSeq(getFillValue(this, Seq.empty))

  private def getFillValue(dt: DataType, acc: Seq[Data]): Seq[Data] = dt match {
    case s: Scalar      => acc :+ s.valueType.fillValue
    case Tuple(es @ _*) => es.flatMap(getFillValue(_, acc))
    case _: Function =>
      val msg = "Can't make fill values for Function, yet."
      throw LatisException(msg)
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

  //import scala.math.Ordering._

  // Note, this will fail eagerly if constructing a Scalar
  // with an invalid value type.
  val valueType: ValueType = this("type").map {
    ValueType.fromName(_) match {
      case Right(v) => v
      case Left(e)  => throw e
    }
  }.getOrElse {
    val msg = s"No value type defined for Scalar: $id"
    throw new RuntimeException(msg)
  }

  /**
   * Converts a string value into the appropriate type for this Scalar.
   */
  def parseValue(value: String): Either[LatisException, Datum] =
    valueType.parseValue(value)

  /**
   * Returns a string representation of the given Datum.
   */
  def formatValue(data: Datum): String = data.asString

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

  override def toString: String = id
}

object Scalar {
  //TODO enforce id or make uid

  /**
   * Construct a Scalar with the given Metadata.
   */
  def apply(md: Metadata): Scalar = new Scalar(md)

  /**
   * Construct a Scalar with the given identifier.
   */
  def apply(id: String): Scalar = Scalar(Metadata(id))
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
