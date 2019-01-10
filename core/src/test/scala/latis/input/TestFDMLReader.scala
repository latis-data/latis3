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
    
    val dataset: Dataset = FDMLReader.parse(loaded).get
    
    assertEquals(dataset.model.arity, 1)
    assertEquals(dataset.metadata.getProperty("name"), None)
    assertEquals(dataset.toString, "/data/timed_see/composite_lya/composite_lya.dat: time -> (la, source)")
    
  }
  
  //TODO: this is just scaffolding for a future test, operations currently not being parsed.
  @Test
  def testWithOperations = {
    val xmlFile = 
      """
<dataset name="nrl2_sunspot_darkening" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="columnar-adapter.xsd">
    <adapter class="latis.reader.tsml.agg.TileJoinAdapter"/>
    <operation>
        <join>
            <dataset name="nrl2_sunspot_darkening" uri="${lisird.content.dir}/lasp/nrl2/spotAIndC_fac_1882_2014d_SOLID_7Oct15.txt">
                <!-- Use SPOT67 (67% contribution from Greenwich) from the new data file from Judith. -->
                <adapter class="latis.reader.tsml.ColumnarAdapter"
                    columns=" 0,1,2;3"
                    skipLines="5"
                    delimiter="\s+"/>
                <scalar type="time" units="yyyy MM dd"/>
                <scalar name="ssd" type="real"/>
                <operation>
                    <select>
                        <vname>time</vname>
                        <operator>=</operator>
                        <value>1983-01-01</value>
                    </select>
                </operation>
            </dataset>

            <dataset name="lasp_sunspot_darkening" uri="${lisird.content.dir}/lasp/nrl2/sunspot_blocking_1981-12-01_2016-12-31_final_with_interp.txt">
                <adapter class="latis.reader.tsml.AsciiAdapter"
                    delimiter="\s+"/>
                <scalar type="time" units="yyyy-MM-dd"/>
                <scalar name="ssd" type="real"/>
                <scalar name="stddev" type="real"/>
                <scalar name="quality_flag" type="integer"/>
                <operation>
                    <select>
                        <vname>time</vname>
                        <operator>=</operator>
                        <value>1983-01-01</value>
                    </select>
                    <project>
                        <vid>time</vid>
                        <vid>ssd</vid>
                    </project>
                </operation>
            </dataset>
        </join>
    </operation>
</dataset>
"""
    val loaded = FDMLReader.load(xmlFile) 
    
    val datasetName = (loaded \ "@name").text
    assertEquals(datasetName, "nrl2_sunspot_darkening")
    
    //TODO: Reading of operations not yet implemented
    //val dataset: Dataset = FDMLReader.parse(loaded).get
  }
}
