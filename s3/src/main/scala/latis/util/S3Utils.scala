package latis.util

import scala.jdk.CollectionConverters.*

import blobstore.s3.S3Store
import blobstore.url.*
import cats.effect.Async
import cats.syntax.all.*
import fs2.*
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request

object S3Utils {
  //TODO: generalize for any supported blobstore: azure, box, gcs (google), sftp
  //TODO: special concerns for directory buckets?
  //  https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
  //  "For general purpose buckets, ListObjectsV2 returns objects in lexicographical order based on their key names."
  //  "For directory buckets, ListObjectsV2 does not return objects in lexicographical order."
  //  Do we need to traverse directories and order as we go?

  /** Makes an S3 client. */
  def makeClient(region: Region): S3Client =
    S3Client.builder.region(region).build

  /** Makes an effectful S3 client. */
  //TODO: Async here? delay?
  def makeClientF[F[_] : Async](region: Region): F[S3AsyncClient] =
    Async[F].delay(S3AsyncClient.builder.region(region).build)

  /** Returns an Iterator of keys for the objects in an S3 bucket. */
  def getKeys(client: S3Client, bucket: String, prefix: String): Iterator[String] = {
    val request = ListObjectsV2Request.builder
      .bucket(bucket)
      .prefix(prefix)
      .build

    client.listObjectsV2Paginator(request).iterator.asScala
      .flatMap(resp => resp.contents.asScala.map(_.key))
  }

  /** Effectfully gets a Stream of keys from an S3 bucket. */
  //TODO: take advantage of S3Client's startAfter (e.g. polling for updates), not in S3Store?
  //TODO: optional prefix? default = "/"?
  def getKeysF[F[_] : Async](client: S3AsyncClient, bucketName: String, prefix: String): F[Stream[F, String]] = {
    for {
      // Just keeping the first error out of possibly many. //TODO: risk missing useful error?
      store <- S3Store.builder[F](client).build.leftMap(_.head).liftTo[F]
      host  <- Hostname.parseF[F](bucketName)
    } yield {
      val url = Url("s3", Authority(host), Path(prefix))
      store.list(url, true).map(_.representation.key)
    }
  }

}
