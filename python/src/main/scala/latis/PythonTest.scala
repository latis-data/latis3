package latis

import jep.Jep
import jep.MainInterpreter

object PythonTest extends App {
  MainInterpreter.setJepLibraryPath("/anaconda3/lib/python3.6/site-packages/jep/jep.cpython-36m-darwin.so")

  val jep = new Jep()
  jep.runScript("python/src/main/python/add.py")
  val a = 2
  val b = 3
  // There are multiple ways to evaluate. Let us demonstrate them:
  jep.eval(s"c = add($a, $b)")
  val ans = jep.getValue("c").asInstanceOf[Long]
  println(ans)

  //val ans2 = jep.invoke("add", a, b).asInstanceOf[Int]
  //println(ans2)

}