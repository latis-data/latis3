package latis.output

import latis.data._
import latis.metadata._
import latis.model._
import latis.util._
import java.io._
import scala.collection._
import cats.effect._
import fs2._

/**
 * Prototype writer implementation.
 */
class Writer(out: OutputStream) {
  //NOTE: we don't have ownership of output stream so don't close
  //TODO: encode to bytes or T as opposed to String
  
  private lazy val printWriter = new PrintWriter(out)
  
  /**
   * Output the given Dataset in the desired form.
   */
  def write(dataset: Dataset): Unit = {
    val z = encode(dataset).flatMap(x => 
      Stream.eval( IO(printWriter.println(x)) )
    ).compile.drain.unsafeRunSync()
    //TODO: unsafeRun util?
    printWriter.flush
  }
  
  
  /**
   * Encode the Stream of Samples from the given Dataset
   * as a Stream of Strings.
   */
  def encode(dataset: Dataset): Stream[IO, String] = {
    // Create a Stream with the header
    val header: Stream[Pure, String] = Stream.emit(dataset.toString)
    
    // Encode each Sample as a String in the Stream
    val samples: Stream[IO, String] = dataset.samples
      .flatMap(encodeSample(dataset.model, _))
    
    // Combine the output into a single Stream
    header ++ samples  //++ flush
  }
    
  /**
   * Given a Sample and it's data model, create a Stream of Strings.
   */
  def encodeSample(model: DataType, sample: Sample): Stream[IO, String] = {
    (model, sample) match {
      case (Function(domain, range), Sample(ds, rs)) =>
        (encodeData(domain, ds) ++ encodeData(range, rs))
        .chunkN(2).map(_.toVector.mkString(" -> "))
    }
  }
  
  def encodeData(model: DataType, data: Seq[_]): Stream[IO, String] = {
    val ds = mutable.Stack(data: _*)
    
    def go(dt: DataType): Stream[IO, String] = dt match {
      //TODO: error if ds is empty
      case _: Scalar => Stream.emit(ds.pop.toString)  //TODO: delegate to dt to produce String?
      
      case Tuple(es @ _*) =>
        Stream.emits(es)
              .flatMap(go(_))
              .chunkN(es.length)
              .map(_.toVector.mkString("(", ",", ")"))
      
      case f: Function => ds.pop match {
        case sf: SampledFunction => encodeFunction(f, sf)
        case _ => ??? //Oops, model and data not consistent
      }
    }
    
    go(model)
  }
  
  /**
   * Platform independent new-line.
   */
  val newLine = System.getProperty("line.separator") //"\n"
  //TODO: put in StringUtils?
  
  //nested function
  //TODO: indent  
  def encodeFunction(ftype: Function, function: SampledFunction): Stream[IO, String] = {
    val head: Stream[Pure, String] = Stream.emit(s"{$newLine")
       
    val delim = newLine //TODO: FirstThenOther?
    //TODO: chunkN map to Vector then mkString? need to know length, 
    //  OK since this is a nested Function
    //  can we tell Stream to chunk all?
    val samples: Stream[IO, String] = 
      function.samples.flatMap(encodeSample(ftype, _)).map(_ + delim)
      
    val foot: Stream[Pure, String] = Stream.emit("}")
    
    head ++ samples ++ foot
  }
    
}

//==== Companion Object =======================================================

object Writer {
  
  def write(dataset: Dataset): Unit = {
    val writer = new Writer(System.out)
    writer.write(dataset)
  }
}
