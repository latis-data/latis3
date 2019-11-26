package latis.input

import latis.data._
import latis.dataset.Dataset
import latis.ops._
import latis.util.FdmlUtils
import latis.util.StreamUtils
import org.junit.Assert._
import org.junit.Ignore
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class TestFdmlReader extends JUnitSuite {
  @Test @Ignore //This might have broken with commit c2458afccf7a4bd1ae71ea0dabeea35ce7ea9bea
  def testSimple = {
    val xmlString =
      """<?xml version="1.0" encoding="UTF-8"?>
    <dataset name="composite_lyman_alpha" uri="http://lasp.colorado.edu/data/timed_see/composite_lya/composite_lya.dat" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="text-adapter.xsd">
        <adapter class="latis.input.TextAdapter"
               skipLines="5" delimiter="+" commentCharacter="--?" dataMarker="@" linesPerRecord="3"/>
      <function id="LA" length="1">
         <scalar id="time" type="time" units="yyyyDDD"/>
         <tuple id="irradiance">
             <scalar id="la" type="real" name="LymanAlpha" long_name="Lyman-alpha irradiance" units ="1e11 photons/cm^2/sec"/>
             <scalar id="source" type="integer" name="type" long_name="Source of the data" missing_value = "-999999"/>
         </tuple>
      </function>
    </dataset>"""
    
    val reader: FdmlReader = FdmlReader(xmlString)
    val dataset: Dataset = reader.getDataset
    
    assertEquals(dataset.model.arity, 1)
    assertEquals(dataset.metadata.getProperty("name"), Some("composite_lyman_alpha"))
    assertEquals(dataset.toString, "/data/timed_see/composite_lya/composite_lya.dat: time -> (la, source)")
    
  }
  
  @Test @Ignore //This might have broken with commit c2458afccf7a4bd1ae71ea0dabeea35ce7ea9bea
  def testWithOperations = {
    val xmlString = """<?xml version="1.0" encoding="UTF-8"?>
<dataset name="composite_lyman_alpha" uri="http://lasp.colorado.edu/data/timed_see/composite_lya/composite_lya.dat" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="text-adapter.xsd">
    <adapter class="latis.input.TextAdapter"
        skipLines="5" delimiter="+" commentCharacter="--?" dataMarker="@" linesPerRecord="3"/>
    <function id="LA" length="1">
        <scalar id="time" type="time" units="yyyyDDD"/>
        <tuple id="irradiance">
            <scalar id="la" type="real" name="LymanAlpha" long_name="Lyman-alpha irradiance" units ="1e11 photons/cm^2/sec"/>
            <scalar id="source" type="integer" name="type" long_name="Source of the data" missing_value = "-999999"/>
        </tuple>
    </function>
    <operation>
        <contains>
            <vname>x</vname>
            <value>y</value>
            <value>z</value>
        </contains>
        <groupby>
            <vname>ix</vname>
            <vname>iy</vname>
        </groupby>
        <head/>
        <pivot>
            <valuetype>real</valuetype>
            <value>44</value>
            <value>55</value>
            <vid>44</vid>
            <vid>55</vid>
        </pivot>
        <project>
            <vid>irradiance</vid>
        </project>
        <rename>
            <vname>irradiance</vname>
            <newName>intensity</newName>
        </rename>
        <select>
            <vname>la</vname>
            <operator>+</operator>
            <value>1</value>
        </select>
        <take>1</take>
        <uncurry/>
    </operation>
</dataset>
"""
    val datasetSource: FdmlReader = FdmlReader(xmlString)
    val operations = datasetSource.operations.toList
    
    val containsOp: Contains = operations.collect {
      case op: Contains => op
    }.head
    val containsOperation = Contains("x", (Seq("y", "z"): _*))
    assertEquals(containsOp.vname, containsOperation.vname)
    
    val groupByOp: GroupBy = operations.collect {
      case op: GroupBy => op
    }.head
    val groupByOperation = GroupBy(Seq("ix", "iy"): _*)
    assertEquals(groupByOp.vnames, groupByOperation.vnames)
    
    val pivotOp: Pivot = operations.collect {
      case op: Pivot => op
    }.head
    val pivotOperation = Pivot(Seq("44", "55"), Seq("44", "55"))
    assertEquals(pivotOp.values.head, pivotOperation.values.head)
    
    val projectionOp: Projection = operations.collect {
      case op: Projection => op
    }.head
    val projectionOperation = Projection(Seq("irradiance"): _*)
    assertEquals(projectionOp.vids.head, projectionOperation.vids.head)
    
    val selectionOp: Selection = operations.collect {
      case op: Selection => op
    }.head
    val selectionOperation = Selection("la", "+", "1")
    assertEquals(selectionOp.vname, selectionOperation.vname)
    assertEquals(selectionOp.operator, selectionOperation.operator)
    assertEquals(selectionOp.value, selectionOperation.value)
    
    val uncurryOp: Uncurry = operations.collect {
      case op: Uncurry => op
    }.head
    assertEquals(uncurryOp, Uncurry())
   
  }
  
  @Test
  def validation(): Unit = {
    val fdmlFile = "data.fdml"
    assertTrue(FdmlUtils.validateFdml(fdmlFile).isRight)
  }
  
  @Test
  def fdml_file(): Unit = {
    val ds = Dataset.fromName("data")
      .withOperation(Selection("time", ">=" , "2000-01-02"))
    StreamUtils.unsafeHead(ds.samples)match {
      case Sample(DomainData(Number(t)),RangeData(Integer(b), Real(c), Text(d))) =>
        assertEquals(1, t, 0)
        assertEquals(2, b)
        assertEquals(2.2, c, 0)
        assertEquals("b", d)
    }
  }
}
