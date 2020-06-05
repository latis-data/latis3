package latis.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.util.Implicits._

class IdentifierSpec extends FlatSpec {

  "A valid Identifier" should "compile" in {
    id"myString"
  }
  
  it should "equal the original string when .toString is used" in {
    val id: Identifier = id"myString"
    id.toString should be ("myString")
  }
  
  it should "be able to contain letters, numbers, and underscores" in {
    id"myString_1"
    id"1_myString"
    id"abcABC_123"
    id"123"
    id"__"
  }
  
  "An invalid Identifier" should "not compile" in {
    assertDoesNotCompile("val id = id\"my string\"")
  }
  
}
