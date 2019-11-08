package latis

import java.net.URI
import java.nio.file.Paths

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.implicits._

import latis.util.FdmlUtils

object FdmlValidatorApp extends IOApp {

  sealed trait Validation
  final case object Valid extends Validation
  final case object Invalid extends Validation

  private val usage: IO[Unit] = IO {
    println("No files or URLs specified.")
  }

  private def invalidArg(arg: String): IO[Validation] = IO {
    println(s"Invalid argument: $arg")
    print(System.lineSeparator())
  }.as(Invalid)

  /**
   * Converts user input to a URI.
   *
   * If the input is not a URI (specifically, if it doesn't have a
   * scheme), this will try to treat the input as a file path and
   * produce a file URI.
   */
  private def toUri(path: String): Option[URI] = try {
    val uri = new URI(path)

    if (uri.getScheme() == null) {
      // Assume we were given a file path.
      Option(Paths.get(path).toUri)
    } else {
      Option(uri)
    }
  } catch {
    case _: java.net.URISyntaxException => None
    case _: java.nio.file.InvalidPathException => None
  }

  /** Validates FDML file at given URI. */
  private def validate(uri: URI): IO[Validation] = for {
    _   <- IO(println(s"Validating ${uri.toString()}"))
    res <- FdmlUtils.validateFdml(uri) match {
      case Right(_)  => IO(println("valid")).as(Valid)
      case Left(msg) => IO(println(msg)).as(Invalid)
    }
    _   <- IO(print(System.lineSeparator()))
  } yield res

  def run(args: List[String]): IO[ExitCode] = args match {
    case Nil => usage.as(ExitCode.Error)
    case xs  => xs.traverse { x =>
      toUri(x).map(validate).getOrElse(invalidArg(x))
    }.map { rs =>
      if (rs.forall(_ == Valid)) {
        ExitCode.Success
      } else {
        ExitCode.Error
      }
    }
  }
}
