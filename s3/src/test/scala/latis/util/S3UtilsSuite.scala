package latis.util

import scala.concurrent.duration.Duration

import cats.effect.IO
import cats.effect.Resource
import com.dimafeng.testcontainers.LocalStackV2Container
import munit.catseffect.IOFixture
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import latis.util.S3Utils.*

class S3UtilsSuite extends munit.CatsEffectSuite {

  //Starting the test container is slow.
  override val munitIOTimeout: Duration = Duration(1, "minute")

  private def makeBucket(client: S3AsyncClient, bucket: String) = {
    IO.fromCompletableFuture {
      IO {
        client.createBucket(
          CreateBucketRequest.builder().bucket(bucket).build()
        )
      }
    }
  }

  private def putEmpty(client: S3AsyncClient, bucket: String, key: String) =
    IO.fromCompletableFuture {
      IO {
        client.putObject(
          PutObjectRequest.builder().bucket(bucket).key(key).build(),
          AsyncRequestBody.empty()
        )
      }
    }

  val s3Client: IOFixture[S3AsyncClient] = {
    val localStack: Resource[IO, LocalStackV2Container] = {
      val mkContainer = IO {
        LocalStackV2Container(services = List(Service.S3))
      }.flatTap(container => IO(container.start()))
      Resource.make(mkContainer)(container => IO(container.stop()))
    }

    val client: Resource[IO, S3AsyncClient] = localStack.flatMap { ls =>
      val mkClient = IO {
        S3AsyncClient
          .builder()
          .endpointOverride(ls.endpointOverride(Service.S3))
          .credentialsProvider(ls.staticCredentialsProvider)
          .region(ls.region)
          .build()
      }
      Resource.make(mkClient)(client => IO(client.close()))
    }.evalTap { client =>
      makeBucket(client, "foobar")
        >> putEmpty(client, "foobar", "foo")
        >> putEmpty(client, "foobar", "bar")
        >> makeBucket(client, "barfoo")
        >> putEmpty(client, "foobar", "bar")
        >> putEmpty(client, "foobar", "foo")
    }

    ResourceSuiteLocalFixture("s3-client", client)
  }

  override val munitFixtures = List(s3Client)

  test("ordering") {
    IO(s3Client()).flatTap { client =>
      getKeysF[IO](client, "foobar", "/").flatMap { stream =>
        stream.compile.toList.map {
          case "bar" :: "foo" :: Nil => ()
          case _ => fail("Objects not ordered as expected.")
        }
      }
    }
  }
  
  //=== Test with NOAA data ===//

  val bucket = "noaa-nesdis-swfo-ccor-1-pds"
  val prefix = "SWFO/GOES-19/CCOR-1/ccor1-l3_science/"
  val first = "SWFO/GOES-19/CCOR-1/ccor1-l3_science/2025/02/25/" +
    "sci_ccor1-l3_g19_s20250225T000020Z_e20250225T000048Z_p20250401T234952Z_pub.fits"

  test("pure client") {
    val client = makeClient()
    val key = getKeys(client, bucket, prefix).toList.head
    assertEquals(first, key)
  }

  test("effectful client") {
    for {
      client <- makeClientF[IO]()
      keys   <- getKeysF[IO](client, bucket, prefix)
      last   <- keys.head.compile.last
      key    <- IO.fromOption(last)(fail("Empty list of keys")) //TODO: does this failure work?
    } yield {
      assertEquals(first, key)
    }
  }

  test("chunk size") {
    for {
      client <- makeClientF[IO]()
      keys   <- getKeysF[IO](client, bucket, prefix)
      last   <- keys.chunks.head.compile.last
      chunk  <- IO.fromOption(last)(fail("Empty list of keys")) //TODO: does this failure work?
    } yield {
      assertEquals(1000, chunk.size)
    }
  }

}
