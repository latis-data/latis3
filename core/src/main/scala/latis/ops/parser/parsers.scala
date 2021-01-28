package latis.ops.parser

import atto.Parser

object parsers {

  def scalarArg: Parser[String] = latis.util.dap2.parser.parsers.scalarArg

}
