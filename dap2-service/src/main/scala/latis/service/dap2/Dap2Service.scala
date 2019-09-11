package latis.service.dap2

import java.net.URLDecoder

import cats.effect._
import cats.implicits._
import org.http4s._
import org.http4s.dsl.Http4sDsl

import latis.server.ServiceInterface
import latis.util.dap2.ConstraintParser

class Dap2Service extends ServiceInterface with Http4sDsl[IO] {

  override def routes: HttpRoutes[IO] =
    HttpRoutes.of {
      // I don't know how to get both the dataset and the constraint
      // expression through the http4s DSL, so I use the DSL to handle
      // the routing and get the query myself.
      case req @ GET -> Root =>
        val uri = req.uri.renderString

        // TODO: This is just for demonstration purposes.
        uri.split('?').toList match {
          case ds :: ce :: Nil
              if ds.nonEmpty && ce.nonEmpty =>
            val ceDec = URLDecoder.decode(ce, "UTF-8")
            ConstraintParser.parse(ceDec) match {
              case Left(msg) => BadRequest(msg)
              case Right(_)  => Ok(ceDec)
            }
          case ds :: Nil => Ok(ds)
          case _ => BadRequest()
        }
    }
}
