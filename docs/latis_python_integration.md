# LaTiS Python Integration
We have investigated two different packages for integrating Python code with LaTiS's Scala code: Jep and ScalaPy. 
We are using Jep in the latis-anomaly-detection repo and have tested it in one other repo. A prototype with ScalaPy 
is in the works but has yet to materialize. This page summarizes what we know about Jep and ScalaPy and how we are 
using Python in LaTiS.

## Jep
Jep embeds CPython in Java through the Java Native Interface (JNI). It needs a valid Python environment to run 
(Python >= 2.7, and NumPy >= 1.7 or else it gets compiled without NumPy support) and requires setting a path to its `.so` 
library file in the Scala code. Telling LaTiS where that library file lives is probably Jep's biggest pain point. In both 
v2 and v3, we accomplish this with a JepOperation class. It contains only one function, `setJepPath()`, which gets 
an absolute path to the library file from the configuration. Any operation that calls Python code extends JepOperation.

Jep uses an interpreter to communicate between Python and Scala. Any kind of data can be passed between the two languages 
via the interpreter. Jep automatically converts many primitive data types—Strings, Ints, etc.—for you, but also provides 
a PyJObject abstraction for converting arbitrarily complex data types. See their documentation for more on how that works.

### Using Python code in LaTiS with Jep

Python code is written as Scala Strings that get executed by being passed to the interpreter. The pattern generally goes 
like this (although there are other ways to do it):

Import whatever Python module you want to use
Write your String(s) of Python code then store the result in a Python variable
Retrieve the data from that Python variable in Scala via the interpreter

A simple example looks like this:
```
setJepPath

val interp = new SharedInterpreter

interp.exec("import somePyModule")

interp.exec("x = somePyModule.foo")

val result = interp.getValue("x")
```

To pass data from LaTiS to Python, first set the data in the interpreter then reference it by name in your Python String. 
Note that the interpreter duplicates the Scala data for Python, so memory usage may be a concern.

```
val data = someLatisFunction

interp.set("latisData", data)

interp.exec("y = somePyModule.bar(latisData)")

val result = interp.getValue("y")
```

For a real-world example, see the DetectAnomalies operation in `latis-anomaly-detection`.


## ScalaPy

"ScalaPy allows you to use any Python library from your Scala code with an intuitive API"—the ScalaPy docs. ScalaPy is 
an alternative to Jep which provides much of the same functionality but with some added features. These features could 
prove to be more useful than what Jep offers, but we have not tested ScalaPy with LaTiS, so we don't know yet. ScalaPy 
was built on top of Jep back when we started playing with Python integration. It was only in a recent version release, 
v0.4.0, that Jep was removed from their equation. With v0.4.0, ScalaPy completely rewrote from scratch their JVM 
interface to Python to share all of its logic with the Scala Native backend by binding directly to CPython with JNA. 
Their approach offers a complete ecosystem (use any Python library), strong typing (compile-time error checking of 
Python code), and performant interoperability. It should be noted, however, that ScalaPy is a less mature project 
than Jep, with notably less community help and potentially more instability due to the recent rewrite.

Using Python code in LaTiS with ScalaPy

Instead of doing everything with Strings like Jep, ScalaPy mainly calls Python code through its Scala API. The primary 
entrypoint into the Python interpreter from ScalaPy is `py.global`, which acts similarly to Scala.js's `js.Dynamic.global` 
to provide a dynamically-typed interface for the interpreter's global scope. For example, we can create a Python range 
with the range() method, and calculate the sum of its elements with sum().

```
val list = py.Dynamic.global.range(1, 3 + 1)  // list: py.Dynamic = range(1, 4)

val listSum = py.Dynamic.global.sum(list)   // listSum: py.Dynamic = 6
```

Modules are imported with the `py.module` method, then the modules' methods are exposed through their import objects. 
For instance, the first Jep example from above would translate to ScalaPy like so:

val spm = py.module("somePyModule")

val result = spm.foo.as[myScalaType]

Note that to convert Python values back into Scala, we use the .as method and pass in the type we want.

To pass data from LaTiS to Python, ScalaPy offers the .toPythonProxy method along with the .as method. The former 
automatically converts certain Scala types into their corresponding Python types, while the latter ties in with their 
whole type facades approach that allows you to define static type definitions for more complex types. The second Jep 
example from above would translate to ScalaPy like so:

```
val data = someLatisFunction

val result = spm.bar(data.toPythonProxy).as[myScalaType]
```

There will be times when Python code you want to use can't be expressed through ScalaPy's Scala API. For those times, 
ScalaPy offers the ability to interpret Strings as Python much like Jep does. This is done with either the py"" 
interpolator or the py.eval("") method. See their docs for more on this.

No real-world LaTiS example exists because we have not tested ScalaPy with LaTiS.
