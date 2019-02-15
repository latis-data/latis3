# 0001 - Scala programming language.

* Status: proposed  
* Date: 2019-02-12


## Context and Problem Statement

The hybrid object-oriented/functional language Scala is ideally suited for developing LaTiS.

## Decision Drivers <!-- optional -->

* merits of functional programming languages
* merits of strongly typed languages
* availability of developers
* consensus of community of scientific programmers
* suitability for processing large data sets
* concepts involving code correctnes
* transformations to data are explicit and can be reasoned about
* support for DSLs (Domain Specific Languages)

## Considered Options

* Java
* Python
* Haskell

## Decision Outcome

Chosen option: Scala is the language-of-choice for LaTiS development.  The only decision driver above that is not in alignment with this decision is the fact that Python is the preferred language for most scientific programmers.

### Positive Consequences <!-- optional -->

* immutable data types in Scala reduce the challenges of introducing parallelism
* for an extensible library like LaTiS a functional programming language like Scala provides better abstractions and tools to reason about transformations of large data sets
* a strongly typed language like Scala helps to prevent code rot and reduces the chance that defects will slip into the code base
* well designed Scala applications are based on sound software engineering principles even though they may take more effort to build than similar Java or Python programs
* that being said, Scala programs tend to be easier to maintain and refactor which actually minimizes effort over the life of a project
* LaTiS is primarily a framework for transforming large datasets in a performant manner, this meshes nicely with the functional approach that emphasizes creating abstractions that do things to data rather than creating specific recipes for specific use cases
* Apache Spark, written in Scala, is the leading framework for processing large amounts of data and is becoming on of the primary framework for implementing machine learning algorithms
* Scala supports the creation of DSLs which will allow users of LaTiS to specify processing instructions for datasets without themselves nedding to learn Scala
* the full gamut of Java libraies is available to Scala developers
* many successful Java libraries eventually get re-written in Scala
* Scala encourages software craft-people to follow sound engineering principles instead of just developing something that just gets the job done, this is probably the most important factor in choosing Scala, but the hardest to document


### Negative Consequences 

* Scala developers are not as common as Python developers, especially in the field of scientific computing
* Python is clearly the dominant language used by scientific programmers

## Pros and Cons of the Options 

### Java

The original version of LaTiS was written in Java.

* Pro, Java is a very mainstream programming language
* Pro, newer versions of Java are becoming more functional
* Con, Java is still too focused on the object-oriented paradigm
* Con, despite the improvements being made to Java, Java has too many inherent design limitations

### Python

Python is currently the leading language for scientific programming and machine learning.

* Pro, Python is a very mainstream programming language
* Pro, PySpark supports use of Apache Spark
* Con, Python is a dynamically typed language which results in code that is more fragile, less maintainable, more dependant on good unit tests, and less suited for building robust frameworks
* Con, Python was created to lower the learning curve for programmers who are not software engineers

### Haskell

Haskell is a pure functional programming language that may in time become the language of choice for future versions of LaTiS

* Pro, as a pure functional language Haskell has the potential to supplant Scala as a reference implementation of the functional data model
* Pro, Haskell has been around longer than Scala and may outlive Scala
* Con, Haskell currently does not have the framework support that Scala (and Java) do such as Spark
* Con, there are even less Haskell developers available than Scala developers
