package latis.input

import latis.metadata.Metadata
import latis.model.Dataset
import latis.model.DataType
import latis.model.Function
import latis.model.Scalar
import latis.output.Writer

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
    val xmlFile = """<?xml version="1.0" encoding="UTF-8"?>
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
        <take arg="1"/>
        <uncurry/>
    </operation>
</dataset>
"""
    val loaded = FDMLReader.load(xmlFile) 
    val datasetSource: AdaptedDatasetSource = FDMLReader.parse(loaded).get
    val operations = datasetSource.operations
    assertEquals(operations.toString, "List(Contains(x,WrappedArray(non-empty iterator)), GroupBy(Stream(ix, ?)), Pivot(Stream(44, ?),Stream(44, ?)), Projection(Stream(irradiance, ?)), Selection(la,+,1), Uncurry())")
  }
}
