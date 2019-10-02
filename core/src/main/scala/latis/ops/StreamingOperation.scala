package latis.ops


/**
 * Operations that implement the StreamingOperation trait
 * can be applied one Sample at a time. These Operations
 * can thus be composed with a MappingOperation (applied first)
 */
trait StreamingOperation {
  /*
   * TODO: "streaming" might not be the right abstraction for composition
   * e.g. groupBy can stream in but not out
   */
  
  //TODO: return this.type?
  def compose(mappingOp: MapOperation): StreamingOperation
}
