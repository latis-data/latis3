package latis.util

class TestClass
object TestClass {
  val test = "hello"
}

class ReflectionUtilsSuite extends munit.FunSuite {
  test("get companion object") {
    val obj = ReflectionUtils.getCompanionObject("latis.util.TestClass").asInstanceOf[TestClass.type]
    assertEquals(obj.test, "hello")
  }
}

