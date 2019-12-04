package latis

import java.net.URI
import java.nio.file.Paths

import latis.util.FdmlUtils

object FdmlValidatorApp {

  sealed trait Validation
  final case object Valid extends Validation
  final case object Invalid extends Validation

  private def usage(): Unit = println("No files or URLs specified.")

  private def invalidArg(arg: String): Validation = {
    println(s"Invalid argument: $arg")
    print(System.lineSeparator())

    Invalid
  }

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
  private def validate(uri: URI): Validation = {
    println(s"Validating ${uri.toString()}")

    val res = FdmlUtils.validateFdml(uri) match {
      case Right(_)  => { println("valid"); Valid }
      case Left(err) => { println(err.message); Invalid }
    }

    print(System.lineSeparator())

    res
  }

  def main(args: Array[String]): Unit = args.toList match {
    case Nil => usage()
    case xs  =>
      val results = xs.map { x =>
        toUri(x).fold(invalidArg(x))(validate)
      }

      if (results.forall(_ == Valid)) {
        System.exit(0)
      } else {
        System.exit(1)
      }
  }
}
