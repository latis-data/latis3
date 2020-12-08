package latis.fits

sealed trait Data {}

class BinaryTableData extends Data {}

class AsciiTableData extends Data {}

class ImageData extends Data {}
