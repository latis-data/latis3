package latis.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import latis.util.Identifier._

class IdentifierSpec extends AnyFlatSpec {

  "A valid Identifier" should "compile" in {
    id"myString"
  }

  it should "equal the original string when .asString is used" in {
    val id: Identifier = id"myString"
    id.asString should be ("myString")
  }

  it should "be able to contain letters, numbers (not the first character), and underscores" in {
    id"myString_1"
    id"_myString"
    id"abcABC_123"
    id"_123"
    id"__"
    id"myString.another._1" //TODO: remove this once namespacing is finalized and dots are disallowed

  }

  "An invalid Identifier" should "not compile" in {
    assertDoesNotCompile("val id = id\"my string\"") //contains a space
    assertDoesNotCompile("val id = id\"123_abc\"")   //starts with a number
    assertDoesNotCompile("Identifier(\"myString\")") //can't construct them like this
  }

}
