package latis.input

import latis.model.Dataset
import latis.ops.Operation
import latis.ops.UnaryOperation
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import java.util.ServiceLoader


/**
 * A DatasetSource is a provider of a LaTiS Dataset
 * as opposed to an Adapter that just provides data.
 * Provided Operations should be applied lazily to the Dataset.
 */
trait DatasetSource {
  
  /**
   * Optionally return the Dataset with the given Operations applied to it.
   * If the DatasetSource implementation does not support the given dataset name
   * return Null. A failure to read the data could result in an empty Dataset
   * as opposed to Null.
   */
  //def getDataset(name: String, operations: Seq[UnaryOperation] = Seq.empty): Option[Dataset]
  def getDataset(name: String): Option[Dataset]

}

object DatasetSource {
  
  /**
   * Find the DatasetSource that can provide the requested dataset by name
   * among the DatasetSources registered in META-INF/services/ of jar files 
   * in the classpath.
   * If found, get the Dataset with the given operations applied. Otherwise
   * throw a RuntimeException.
   */
  //def getDataset(name: String, operations: Seq[UnaryOperation]): Dataset = {
  def getDataset(name: String): Dataset = {
    ServiceLoader.load(classOf[DatasetSource]).asScala
      .flatMap(_.getDataset(name)).headOption.getOrElse {
        throw new RuntimeException(s"Failed to find source for dataset: name")
      }
  }
  
  /*
   * TODO: apply to AdaptedDatasetSource =====>>>> DatasetReader.read
   * e.g. FDML Reader
   * do we need the DatasetSourceProvider layer?
   * 
   * consider custom dataset "reader" for a specific dataset as a subclass of Dataset
   *   no getDataset
   * consider a Reader that takes a URI
   *   e.g. FdmlReader, NetcdfReader
   *   not quite suitable for a service loader: 
   *     can't take any identifier
   *     assumes URI is to the expected type
   *   should we simply have a Reader class? enforce the idiom
   *     separable from DatasetSource
   *     would it have to be?
   *     could be a DatasetSource without being discoverable via a ServiceLoader?
   *       but supports getDataset(id): Option
   *   FdmlDataset, NetcdfDataset?
   *     construct with uri, ops
   *     fdml is special in that the uri is the descriptor which provides the data location
   *     ok, just metadata, each ultimately have to provide all
   *     nc md could have link to external metadata that we could pull in
   *     ?resolves complication of ADS uri in FdmlReader?
   *     but NetcdfDataset seems to imply something; NetcdfReader seems better
   *     DatasetReader?
   *       read(uri): Dataset? Option?
   *     consider DSL
   *       what would be the most convenient way to construct a dataset from a uri?
   *       use custom scheme that maps to reader?
   *       or suffix of file, or optional mime type arg?
   *       Dataset.fromName/URL: Option[Dataset]
   *       
   * 
   * Do we ever need a handle on the "reader" to close it?
   *   would having a NetcdfDataset make it that easier?
   *     but would be lost after an operation
   * is there a useful abstraction for shared code?
   *   e.g. getDataset in AdaptedDatasetSource
   *   alternate smart Dataset constructor
   *     uri, adapter, metadata, model, ops
   *     
   * 
   */
  
//TODO: try CacheManager first
//TODO: DatasetSourceProvider to Look for a matching "reader" property.
//    LatisProperties.get(s"reader.${datasetId}.class") match {
//      case Some(s) =>
//        Class.forName(s).getConstructor().newInstance().asInstanceOf[DatasetSource]
//    }
}





