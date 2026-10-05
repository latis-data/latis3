package latis.ops

import fs2.Stream
import munit.CatsEffectSuite

import latis.data.*
import latis.metadata.Metadata
import latis.model.* 

class ExcludeMissingSuite extends CatsEffectSuite {
  private lazy val scalarWithFill    = Scalar.fromMetadata(Metadata("id" -> "swf", "type" -> "int", "fillValue" -> "-1"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val scalarWithMissing = Scalar.fromMetadata(Metadata("id" -> "swm", "type" -> "int", "missingValue" -> "-9"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val scalarWithMissingNaN = Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "float", "missingValue" -> "NaN"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val scalar = Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "float"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val tuple: Tuple = Tuple.fromElements(
    scalarWithMissing,
    scalarWithFill
  ).fold(fail("failed to construct tuple", _), identity)
  private lazy val function: Function = Function.from(scalarWithFill, scalarWithMissing)
    .fold(fail("failed to construct function", _), identity)
  private lazy val functionWTuple: Function = Function.from(scalarWithFill, tuple)
    .fold(fail("failed to construct function", _), identity)
  private lazy val nestedFunction: Function = Function.from(
    scalarWithFill,
    function
  ).fold(fail("failed to construct function", _), identity)
  private lazy val tupleWFunc1: Tuple = Tuple.fromElements(
    scalarWithMissing,
    function
  ).fold(fail("failed to construct tuple", _), identity)
  private lazy val tupleWFunc2: Tuple = Tuple.fromElements(
    function,
    scalarWithMissing
  ).fold(fail("failed to construct tuple", _), identity)
  private lazy val tupleWFunc3: Tuple = Tuple.fromElements(
    function,
    function
  ).fold(fail("failed to construct tuple", _), identity)
  private lazy val nestedFunctionWTuple: Function = Function.from(
    scalarWithFill,
    functionWTuple
  ).fold(fail("failed to construct function", _), identity)
  private lazy val tripleNestedFunc: Function = Function.from(
    scalarWithFill,
    nestedFunction
  ).fold(fail("failed to construct function", _), identity)

  test("scalar with NaN") {
    val model = Function.from(Index(), scalarWithMissingNaN)
      .fold(fail("failed to construct function", _), identity)

    val samples = Stream.emits(List(
      Sample(DomainData(), RangeData(0)),
      Sample(DomainData(), RangeData(Float.NaN)),
      Sample(DomainData(), RangeData(2))
    ))

    ExcludeMissing().pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(), RangeData(0)),
        Sample(DomainData(), RangeData(2))
      )
      assertEquals(samples, expected)
    }
  }

  test("scalar with no missing or fill value") {
    val model = Function.from(Index(), scalar)
      .fold(fail("failed to construct function", _), identity)

    val samples = Stream.emits(List(
      Sample(DomainData(), RangeData(0)),
      Sample(DomainData(), RangeData(Float.NaN)),
      Sample(DomainData(), RangeData(Float.NaN))
    ))

    ExcludeMissing().pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(), RangeData(0))
      )
      assertEquals(samples, expected)
    }
  }

  test("samples with scalars") {
    val model = Function.from(Index(), scalarWithMissing)
      .fold(fail("failed to construct function", _), identity)

    val samples = Stream.emits(List(
      Sample(DomainData(), RangeData(0)),
      Sample(DomainData(), RangeData(-1)),
      Sample(DomainData(), RangeData(-9)),
      Sample(DomainData(), RangeData(NullData))
    ))

    ExcludeMissing().pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(), RangeData(0)),
        Sample(DomainData(), RangeData(-1))
      )
      assertEquals(samples, expected)
    }
  }

  test("samples with tuples") {
    val model = Function.from(Index(), tuple)
      .fold(fail("failed to construct function", _), identity)

    val samples = Stream.emits(List(
      Sample(DomainData(), RangeData(0, 0)),
      Sample(DomainData(), RangeData(-9, 4)),
      Sample(DomainData(), RangeData(0, -1))
    ))

    ExcludeMissing().pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(), RangeData(0, 0))
      )
      assertEquals(samples, expected)
    }
  }

  test("function with scalar -> scalar") {
    val model = function

    val samples = Stream.emits(List(
      Sample(DomainData(2), RangeData(4)),
      Sample(DomainData(0), RangeData(-9))
    ))

    ExcludeMissing().pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(2), RangeData(4))
      )
      assertEquals(samples, expected)
    }
  }

  test("function with scalar -> tuple") {
    val model = functionWTuple

    val samples = Stream.emits(List(
      Sample(DomainData(2), RangeData(4, 2)),
      Sample(DomainData(1), RangeData(-9, 4)),
      Sample(DomainData(3), RangeData(0, -1))
    ))

    ExcludeMissing().pipe(model)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(2), RangeData(4, 2))
      )
      assertEquals(samples, expected)
    }
  }  

  test("nested function") {
    val innerSample1 = Sample(DomainData(2), RangeData(4))
    val innerSample2 = Sample(DomainData(0), RangeData(-9))

    val samples = Stream.emits(List(
      Sample(DomainData(0), RangeData(
        SampledFunction(List(
          innerSample1,
          innerSample2
        ))
      )),
      Sample(DomainData(1), RangeData(
        SampledFunction(List(
          innerSample1,
          innerSample1
        ))
      )),
      Sample(DomainData(2), RangeData(
        SampledFunction(List(
          innerSample2,
          innerSample2
        ))
      )),
      Sample(DomainData(3), RangeData(NullData))
    ))

    ExcludeMissing().pipe(nestedFunction)(samples).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(0), RangeData(
          SampledFunction(List(innerSample1))
        )),
        Sample(DomainData(1), RangeData(
          SampledFunction(List(
            innerSample1,
            innerSample1
          ))
        )),
        Sample(DomainData(2), RangeData(
          SampledFunction(List())
        ))
      )
      assertEquals(samples, expected)
    }
  }

  test("function within a tuple - (scalar, function)") {
    val model = Function.from(scalarWithFill, tupleWFunc1)
      .fold(fail("failed to construct function", _), identity)

    val innerSample1 = Sample(DomainData(0), RangeData(0))
    val innerSample2 = Sample(DomainData(-1), RangeData(4))
    val innerSample3 = Sample(DomainData(8), RangeData(-9))

    val samples = Stream.emits(List(
      Sample(DomainData(0), RangeData(4, SampledFunction(
        List(innerSample1, innerSample1, innerSample2)
      ))),
      Sample(DomainData(1), RangeData(-9, SampledFunction(
        List(innerSample3, innerSample2, innerSample1)
      ))),
      Sample(DomainData(2), RangeData(0, SampledFunction(
        List(innerSample1, innerSample2, innerSample3)
      ))),
      Sample(DomainData(3), RangeData(2, SampledFunction(
        List(innerSample3, innerSample3, innerSample3)
      )))
    ))

    ExcludeMissing().pipe(model)(samples).compile.toList.map { s =>
      val expected = List(
        Sample(DomainData(0), RangeData(4, SampledFunction(
          List(
            innerSample1, innerSample1, innerSample2
          ))
        )),
        Sample(DomainData(2), RangeData(0, SampledFunction(
          List(
            innerSample1, innerSample2
          ))
        )),
        Sample(DomainData(3), RangeData(2, SampledFunction(List()))
        ))
      assertEquals(s, expected)
    }
  }

  test("function within a tuple - (function, scalar)") {
    val model = Function.from(scalarWithFill, tupleWFunc2)
      .fold(fail("failed to construct function", _), identity)

    val innerSample1 = Sample(DomainData(0), RangeData(0))
    val innerSample2 = Sample(DomainData(-1), RangeData(4))
    val innerSample3 = Sample(DomainData(8), RangeData(-9))

    val samples = Stream.emits(List(
      Sample(DomainData(0), RangeData(
        SampledFunction(List(
          innerSample1, innerSample1, innerSample2
        )), 0)
      ),
      Sample(DomainData(1), RangeData(
        SampledFunction(List(
          innerSample3, innerSample2, innerSample1
        )), -9)
      ),
      Sample(DomainData(2), RangeData(
        SampledFunction(List(
          innerSample1, innerSample2, innerSample3
        )), 2)
      ),
      Sample(DomainData(3), RangeData(
        SampledFunction(List(
          innerSample3, innerSample3, innerSample3
        )), 3)
      )
    ))

    ExcludeMissing().pipe(model)(samples).compile.toList.map { s =>
      val expected = List(
        Sample(DomainData(0), RangeData(
          SampledFunction(List(
            innerSample1, innerSample1, innerSample2
          )), 0)
        ),
        Sample(DomainData(2), RangeData(
          SampledFunction(List(
            innerSample1, innerSample2
          )), 2)
        ),
        Sample(DomainData(3), RangeData(
          SampledFunction(List()), 3)
        )
      )
      assertEquals(s, expected)
    }
  }

  test("function within a tuple - (function, function)") {
    val model = Function.from(scalarWithFill, tupleWFunc3)
      .fold(fail("failed to construct function", _), identity)

    val innerSample1 = Sample(DomainData(0), RangeData(0))
    val innerSample2 = Sample(DomainData(-1), RangeData(4))
    val innerSample3 = Sample(DomainData(8), RangeData(-9))

    val samples = Stream.emits(List(
      Sample(DomainData(0), RangeData(
        SampledFunction(List(
          innerSample1, innerSample2, innerSample1
        )),
        SampledFunction(List(
          innerSample3, innerSample2, innerSample1
        ))
      )),
      Sample(DomainData(1), RangeData(
        SampledFunction(List(
          innerSample1, innerSample2, innerSample3
        )),
        SampledFunction(List(
          innerSample3, innerSample3, innerSample3
        ))
      ))
    ))

    ExcludeMissing().pipe(model)(samples).compile.toList.map { s =>
      val expected = List(
        Sample(DomainData(0), RangeData(
          SampledFunction(List(
            innerSample1, innerSample2, innerSample1
          )),
          SampledFunction(List(
            innerSample2, innerSample1
          ))
        )),
        Sample(DomainData(1), RangeData(
          SampledFunction(List(
            innerSample1, innerSample2
          )),
          SampledFunction(List())
        ))
      )
      assertEquals(s, expected)
    }
  }

  test("nested function with tuple") {
    val innerSample1 = Sample(DomainData(0), RangeData(1, 1))
    val innerSample2 = Sample(DomainData(-1), RangeData(-9, 2))
    val innerSample3 = Sample(DomainData(8), RangeData(3, -1))

    val sample = Stream.emits(List(
      Sample(DomainData(0), RangeData(SampledFunction(
        List(innerSample1, innerSample1, innerSample1)
      ))),
      Sample(DomainData(1), RangeData(SampledFunction(
        List(innerSample2, innerSample3, innerSample1)
      ))),
      Sample(DomainData(2), RangeData(SampledFunction(
        List(innerSample2, innerSample3, innerSample3)
      )))
    ))

    ExcludeMissing().pipe(nestedFunctionWTuple)(sample).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(0), RangeData(
          SampledFunction(List(
            innerSample1, innerSample1, innerSample1
          ))
        )),
        Sample(DomainData(1), RangeData(
          SampledFunction(List(
            innerSample1
          ))
        )),
        Sample(DomainData(2), RangeData(
          SampledFunction(List())
        ))
      )
      assertEquals(samples, expected)
    }
  }

  test("triply nested function") {
    val innerSample1 = Sample(DomainData(0), RangeData(1))
    val innerSample2 = Sample(DomainData(-1), RangeData(-9))
    
    val sample = Stream.emits(List(
      Sample(DomainData(0), RangeData(SampledFunction(List(
        Sample(DomainData(0), RangeData(
          SampledFunction(List(innerSample1)))),
        Sample(DomainData(1), RangeData(
          SampledFunction(List(innerSample2))))
      )))),
      Sample(DomainData(1), RangeData(SampledFunction(List(
        Sample(DomainData(0), RangeData(
          SampledFunction(List(innerSample1)))),
        Sample(DomainData(1), RangeData(
          SampledFunction(List(innerSample1))))
      )))),
      Sample(DomainData(2), RangeData(SampledFunction(List(
        Sample(DomainData(0), RangeData(
          SampledFunction(List(innerSample2)))),
        Sample(DomainData(1), RangeData(
          SampledFunction(List(innerSample2))))
      ))))
    ))

    ExcludeMissing().pipe(tripleNestedFunc)(sample).compile.toList.map { samples =>
      val expected = List(
        Sample(DomainData(0), RangeData(SampledFunction(List(
            Sample(DomainData(0), RangeData(
              SampledFunction(List(innerSample1)))),
            Sample(DomainData(1), RangeData(
              SampledFunction(List())
            ))
          ))
        )),
        Sample(DomainData(1), RangeData(SampledFunction(List(
            Sample(DomainData(0), RangeData(
              SampledFunction(List(innerSample1)))),
            Sample(DomainData(1), RangeData(
              SampledFunction(List(innerSample1))))
          ))
        )),
        Sample(DomainData(2), RangeData(SampledFunction(List(
            Sample(DomainData(0), RangeData(
              SampledFunction(List()))),
            Sample(DomainData(1), RangeData(
              SampledFunction(List())))
          ))
        ))
      )
      assertEquals(samples, expected)
    }
  }
} 
