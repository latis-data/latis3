package latis.catalog

import scala.xml.XML

import blobstore.s3.S3Blob
import blobstore.s3.S3Store
import blobstore.url.Url
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import software.amazon.awssdk.services.s3.S3AsyncClient

import latis.dataset.Dataset
import latis.input.fdml.FdmlReader
import latis.input.fdml.FdmlParser
import latis.ops.OperationRegistry

object S3FdmlCatalog {

  /** Creates a catalog from FDML files in an S3 bucket. */
  def fromS3Client(
    client: S3AsyncClient,
    bucketName: String,
    opReg: OperationRegistry
  ): IO[Catalog] = {
    // Just keeping the first error out of possibly many.
    val store = S3Store.builder[IO](client).build.leftMap(_.head).liftTo[IO]

    val bucket = Url.parseF[IO](s"s3://$bucketName")

    (store, bucket, opReg.pure[IO])
      .flatMapN(bucketToDatasets)
      .map(Catalog.fromFoldable(_))
  }

  private def bucketToDatasets(
    store: S3Store[IO],
    bucket: Url.Plain,
    opReg: OperationRegistry
  ): IO[Vector[Dataset]] = listFdmlFiles(store, bucket)
    .evalMap(urlToDataset(store, _, opReg))
    .compile
    .toVector

  private def listFdmlFiles(
    store: S3Store[IO],
    url: Url.Plain
  ): Stream[IO, Url[S3Blob]] = store.list(url).filter {
    _.path.fileName.filter(_.endsWith(".fdml")).isDefined
  }

  private def parseFdml(
    str: String,
    opReg: OperationRegistry
  ): Either[Throwable, Dataset] =
    FdmlParser.parseXml(XML.loadString(str)).flatMap(FdmlReader.read(_, opReg))

  private def urlToDataset(
    store: S3Store[IO],
    url: Url[S3Blob],
    opReg: OperationRegistry
  ): IO[Dataset] = store.getContents(url).flatMap(parseFdml(_, opReg).liftTo[IO])

}
