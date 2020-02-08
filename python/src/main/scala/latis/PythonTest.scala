package latis

import jep.Jep

object PythonTest extends App {
	val jep = new Jep() //https://github.com/ninia/jep/wiki/FAQ#how-do-i-fix-unsatisfied-link-error-no-jep-in-javalibrarypath
  jep.runScript("src/main/python/add.py")
	val a = 2
	val b = 3
	// There are multiple ways to evaluate. Let us demonstrate them:
	jep.eval(s"c = add($a, $b)")
	val ans = jep.getValue("c").asInstanceOf[Int]
	println(ans)
//	val ans2 = jep.invoke("add", a, b).asInstanceOf[Int]
//	println(ans2)

}