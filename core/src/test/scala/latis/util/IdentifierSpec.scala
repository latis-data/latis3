package latis.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.metadata.Metadata
import latis.model.Scalar
import latis.util.Identifier._

class IdentifierSpec extends FlatSpec {

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
    
  }
  
  "An invalid Identifier" should "not compile" in {
    assertDoesNotCompile("val id = id\"my string\"") //contains a space
    assertDoesNotCompile("val id = id\"123_abc\"")   //starts with a number
    assertDoesNotCompile("Identifier(\"myString\")") //can't construct them like this
  }

  "Identifiers from equal Strings" should "equal each other" in {
    val id1 = id"myID"
    val str = "myID"
    val id2 = Identifier.fromString(str).get
    val id3 = id"myID"

    id1 == id2 should be (true)
    id1 == id3 should be (true)
    id2 == id3 should be (true)

    val ids: List[Identifier] = List(id"myID_1", id"myID_2", id"myID_3")
    //TODO: test checking if id"myID_2" exists in that list

    val s = Scalar(Metadata("type" -> "int"))
    val sId = s.id.fold("")(_.asString)
    print("yay")
    print(sId)
    print("Hmm")
  }
  
}
