package latis.ops

import munit.FunSuite

import latis.data.*
import latis.metadata.Metadata
import latis.model.* 

class ExcludeMissingSuite extends FunSuite {
  private lazy val scalarWithFill    = Scalar.fromMetadata(Metadata("id" -> "swf", "type" -> "int", "fillValue" -> "-1"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val scalarWithMissing = Scalar.fromMetadata(Metadata("id" -> "swm", "type" -> "int", "missingValue" -> "-9"))
    .fold(fail("failed to construct scalar", _), identity)
  private lazy val tuple: Tuple = Tuple.fromElements(
    scalarWithMissing,
    scalarWithFill
  ).fold(fail("failed to construct tuple", _), identity)
  private lazy val function: Function = Function.from(scalarWithFill, scalarWithMissing)
    .fold(fail("failed to construct function", _), identity)
  private lazy val functionWTuple: Function = Function.from(scalarWithFill, tuple)
    .fold(fail("failed to construct function", _), identity)
  // private lazy val nestedTuple: Tuple = Tuple.fromElements(
  //   scalarWithFill,
  //   tuple
  // ).fold(fail("failed to construct tuple", _), identity)
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

  test("samples with scalars") {
    val model = Function.from(scalarWithFill, scalarWithMissing)
      .fold(fail("failed to construct function", _), identity)

    val sample1 = Sample(
      DomainData(),
      RangeData(0)
    )
    val sample2 = Sample(
      DomainData(),
      RangeData(-1)
    )
    val sample3 = Sample(
      DomainData(),
      RangeData(-9)
    )

    val p = ExcludeMissing().predicate(model)
      .fold(fail("failed to construct ExcludeMissing", _), identity)

    assert(!p(sample1))
    assert(!p(sample2))
    assert(p(sample3))
  }

  test("samples with tuples") {
    val model = Function.from(scalarWithFill, tuple)
      .fold(fail("failed to construct function", _), identity)

    val sample1 = Sample(
      DomainData(),
      RangeData(TupleData(0, 0))
    )
    val sample2 = Sample(
      DomainData(),
      RangeData(TupleData(-9, 4))
    )
    val sample3 = Sample(
      DomainData(),
      RangeData(TupleData(0, -1))
    )

    val p = ExcludeMissing().predicate(model)
      .fold(fail("failed to construct ExcludeMissing", _), identity)

    assert(!p(sample1))
    assert(p(sample2))
    assert(p(sample3))
  }

  test("function with scalar -> scalar") {
    val model = function

    val sample1 = Sample(
      DomainData(2),
      RangeData(4)
    )
    val sample2 = Sample(
      DomainData(-1),
      RangeData(-9)
    )

    val p = ExcludeMissing().predicate(model)
      .fold(fail("failed to construct ExcludeMissing", _), identity)

    assert(!p(sample1))
    assert(p(sample2))
  }

  test("function with scalar -> tuple") {
    val model = functionWTuple

    val sample1 = Sample(
      DomainData(2),
      RangeData(TupleData(4, 2))
    )
    val sample2 = Sample(
      DomainData(1),
      RangeData(TupleData(-9, 4))
    )
    val sample3 = Sample(
      DomainData(3),
      RangeData(TupleData(0, -1))
    )

    val p = ExcludeMissing().predicate(model)
      .fold(fail("failed to construct ExcludeMissing", _), identity)

    assert(!p(sample1))
    assert(p(sample2))
    assert(p(sample3))
  }  
  
  // TODO: nested tuples aren't supported
  // test("nested tuple") {
  //   val model = Function.from(scalarWithFill, nestedTuple)
  //     .fold(fail("failed to construct function", _), identity)
  //   println(model)

  //   val sample1 = Sample(
  //     DomainData(3),
  //     RangeData(TupleData(4, TupleData(5, 6)))
  //   )
  //   val sample2 = Sample(
  //     DomainData(0),
  //     RangeData(TupleData(-1, TupleData(3, 4)))
  //   )
  //   val sample3 = Sample(
  //     DomainData(2),
  //     RangeData(TupleData(0, TupleData(-9, 0)))
  //   )
  //   val sample4 = Sample(
  //     DomainData(1),
  //     RangeData(TupleData(1, TupleData(4, -1)))
  //   )

  //   val p = ExcludeMissing().predicate(model)
  //     .fold(fail("failed to construct ExcludeMissing", _), identity)

  //   assert(!p(sample1))
  //   assert(p(sample2))
  //   assert(p(sample3))
  //   assert(p(sample4))
  // }

  test("nested function") {
    val innerSample1 = SampledFunction(List(
      Sample(
        DomainData(2),
        RangeData(4)
      )
    ))
    val innerSample2 = SampledFunction(List(
      Sample(
        DomainData(0),
        RangeData(-9)
      )
    ))

    val sample1 = Sample(
      DomainData(0),
      RangeData(innerSample1)
    )
    val sample2 = Sample(
      DomainData(1),
      RangeData(innerSample2)
    )

    val p = ExcludeMissing().predicate(nestedFunction)
      .fold(fail("failed to construct ExcludeMissing", _), identity)

    assert(!p(sample1))
    assert(p(sample2))
  }

  test("function within a tuple") {
    val model1 = Function.from(scalarWithFill, tupleWFunc1)
      .fold(fail("failed to construct function", _), identity)
    val model2 = Function.from(scalarWithFill, tupleWFunc2)
      .fold(fail("failed to construct function", _), identity)
    val model3 = Function.from(scalarWithFill, tupleWFunc3)
      .fold(fail("failed to construct function", _), identity)

    val innerSample1 = SampledFunction(List(
      Sample(
        DomainData(0),
        RangeData(0)
      )
    ))
    val innerSample2 = SampledFunction(List(
      Sample(
        DomainData(-1),
        RangeData(4)
      )
    ))
    val innerSample3 = SampledFunction(List(
      Sample(
        DomainData(8),
        RangeData(-9)
      )
    ))

    val sample1 = Sample(
      DomainData(0),
      RangeData(TupleData(4, innerSample1))
    )
    val sample2 = Sample(
      DomainData(1),
      RangeData(TupleData(-9, innerSample1))
    )
    val sample3 = Sample(
      DomainData(2),
      RangeData(TupleData(0, innerSample2))
    )
    val sample4 = Sample(
      DomainData(3),
      RangeData(TupleData(2, innerSample3))
    )

    val sample5 = Sample(
      DomainData(0),
      RangeData(TupleData(innerSample1, 0))
    )
    val sample6 = Sample(
      DomainData(1),
      RangeData(TupleData(innerSample1, -9))
    )
    val sample7 = Sample(
      DomainData(2),
      RangeData(TupleData(innerSample2, 2))
    )
    val sample8 = Sample(
      DomainData(3),
      RangeData(TupleData(innerSample3, 3))
    )

    val sample9 = Sample(
      DomainData(0),
      RangeData(TupleData(innerSample1, innerSample1))
    )
    val sample10 = Sample(
      DomainData(1),
      RangeData(TupleData(innerSample1, innerSample2))
    )
    val sample11 = Sample(
      DomainData(2),
      RangeData(TupleData(innerSample3, innerSample1))
    )

    val p1 = ExcludeMissing().predicate(model1)
      .fold(fail("failed to construct ExcludeMissing", _), identity)
    val p2 = ExcludeMissing().predicate(model2)
      .fold(fail("failed to construct ExcludeMissing", _), identity)
    val p3 = ExcludeMissing().predicate(model3)
      .fold(fail("failed to construct ExcludeMissing", _), identity)

    assert(!p1(sample1))
    assert(p1(sample2))
    assert(!p1(sample3))
    assert(p1(sample4))

    assert(!p2(sample5))
    assert(p2(sample6))
    assert(!p2(sample7))
    assert(p2(sample8))

    assert(!p3(sample9))
    assert(!p3(sample10))
    assert(p3(sample11))
  }

  test("nested function with tuple") {
    val innerSample1 = SampledFunction(List(
      Sample(
        DomainData(0),
        RangeData(TupleData(1, 1))
      )
    ))
    val innerSample2 = SampledFunction(List(
      Sample(
        DomainData(-1),
        RangeData(TupleData(-9, 2))
      )
    ))
    val innerSample3 = SampledFunction(List(
      Sample(
        DomainData(8),
        RangeData(TupleData(3, -1))
      )
    ))

    val sample1 = Sample(
      DomainData(0),
      RangeData(innerSample1)
    )
    val sample2 = Sample(
      DomainData(1),
      RangeData(innerSample2)
    )
    val sample3 = Sample(
      DomainData(2),
      RangeData(innerSample3)
    )

    val p = ExcludeMissing().predicate(nestedFunctionWTuple)
      .fold(fail("failed to construct ExcludeMissing", _), identity)

    assert(!p(sample1))
    assert(p(sample2))
    assert(p(sample3))
  }

  test("triply nested function") {
    val innerSample1 = SampledFunction(List(
      Sample(
        DomainData(0),
        RangeData(1)
      )
    ))
    val innerSample2 = SampledFunction(List(
      Sample(
        DomainData(-1),
        RangeData(TupleData(-9))
      )
    ))

    val sample1 = Sample(
      DomainData(0),
      RangeData(SampledFunction(List(
        Sample(
          DomainData(0),
          RangeData(innerSample1)
        )
      )))
    )
    val sample2 = Sample(
      DomainData(1),
      RangeData(SampledFunction(List(
        Sample(
          DomainData(1),
          RangeData(innerSample2)
        )
      )))
    )

    val p = ExcludeMissing().predicate(tripleNestedFunc)
      .fold(fail("failed to construct ExcludeMissing", _), identity)

    assert(!p(sample1))
    assert(p(sample2))

  }
} 
