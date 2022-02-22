package latis.output

import java.io.OutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

import cats.effect.IO
import cats.effect.Resource
import fs2.Stream

import latis.data.Data
import latis.data.DomainData
import latis.data.RangeData
import latis.data.Sample
import latis.data.Text
import latis.dataset.Dataset
import latis.util.LatisException

/**
 * Encodes a dataset to a stream of bytes representing a ZIP file.
 *
 * The dataset being encoded is assumed to be of the form `entryName →
 * bytes`, where `entryName` contains the name of the ZIP entry and
 * `bytes` contains the bytes to be written in that entry.
 *
 * An empty dataset will encode to an empty ZIP file.
 */
final class ZipEncoder extends Encoder[IO, Byte] {

  override def encode(dataset: Dataset): Stream[IO, Byte] =
    fs2.io.readOutputStream[IO](4096) {
      mkZipOutputStream(_).use { zos =>
        dataset.samples.evalMap(addZipEntry(zos, _)).compile.drain
      }
    }

  private def addZipEntry(
    zos: ZipOutputStream,
    sample: Sample
  ): IO[Unit] = sample match {
    case Sample(DomainData(Text(name)), RangeData(bv: Data.BinaryValue)) =>
      val entry = new ZipEntry(name)
      val bytes = bv.value

      IO.blocking(zos.putNextEntry(entry)) >>
      IO.blocking(zos.write(bytes, 0, bytes.length)) >>
      IO.blocking(zos.closeEntry())
    case _ => IO.raiseError(LatisException("Unsupported dataset"))
  }

  private def mkZipOutputStream(
    os: OutputStream
  ): Resource[IO, ZipOutputStream] =
    Resource.fromAutoCloseable(IO(new ZipOutputStream(os)))
}
