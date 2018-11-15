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
  def test = {
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
    assertEquals(dataset.toString, "Some(/data/timed_see/composite_lya/composite_lya.dat): time -> (la, source)")
    
  }
}