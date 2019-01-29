package latis.input

import latis.metadata.Metadata
import latis.model.Dataset
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.output.Writer
import latis.ops._

import java.net.URI

import scala.xml._

import org.junit.Test
import org.junit.Assert._

class TestFDMLReader {
  @Test
  def testSimple = {
    val xmlFile =
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
    
    val loaded = FDMLReader.load(xmlFile) 
    
    val datasetName = (loaded \ "@name").text
    assertEquals(datasetName, "composite_lyman_alpha")
    
    val datasetSource: AdaptedDatasetSource = FDMLReader.parse(loaded).get
    
    val dataset: Dataset = datasetSource.getDataset(Seq.empty)
    
    assertEquals(dataset.model.arity, 1)
    assertEquals(dataset.metadata.getProperty("name"), Some("composite_lyman_alpha"))
    assertEquals(dataset.toString, "/data/timed_see/composite_lya/composite_lya.dat: time -> (la, source)")
    
  }
  
  //TODO: this is just scaffolding for a future test, operations currently not being parsed.
  @Test
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
    val loaded = FDMLReader.load(xmlString) 
    val datasetSource: AdaptedDatasetSource = FDMLReader.parse(loaded).get
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
}
