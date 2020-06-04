package latis.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.util.Implicits._

class IdentifierSpec extends FlatSpec {

  "A valid Identifier" should "compile" in {
    identifier"myString"
  }
  
  it should "equal the original string when .toString is used" in {
    val id: Identifier = identifier"myString"
    id.toString should be ("myString")
  }
  
  //TODO: Ideally, we would like to not have to use .toString
  it should "not equal the original string when .toString is not used" in {
    val id: Identifier = identifier"myString"
    id should not be ("myString")
  }
  
  it should "be able to contain letters, numbers, and underscores" in {
    identifier"myString_1"
    identifier"1_myString"
    identifier"abcABC_123"
    identifier"123"
    identifier"_"
  }
  
  "An invalid Identifier" should "not compile" in {
    assertDoesNotCompile("val id = identifier\"my string\"")
  }
  
}
