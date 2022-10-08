package latis.catalog

import java.nio.file.Paths

import scala.concurrent.duration.Duration

import cats.effect.IO
import cats.effect.Resource
import com.dimafeng.testcontainers.LocalStackV2Container
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest

final class S3FdmlCatalogSuite extends munit.CatsEffectSuite {

  // Starting the test container is slow.
  override val munitTimeout: Duration = Duration(1, "minute")

  val s3Client = {
    val localStack = {
      val mkContainer = IO {
        LocalStackV2Container(services = List(Service.S3))
      }.flatTap(container => IO(container.start()))
      Resource.make(mkContainer)(container => IO(container.stop()))
    }

    val client = localStack.flatMap { ls =>
      val mkClient = IO {
        S3AsyncClient
          .builder()
          .endpointOverride(ls.endpointOverride(Service.S3))
          .credentialsProvider(ls.staticCredentialsProvider)
          .region(ls.region)
          .build()
      }
      Resource.make(mkClient)(client => IO(client.close()))
    }.evalTap { cl =>
      IO.fromCompletableFuture {
        IO {
          cl.createBucket(
            CreateBucketRequest
              .builder()
              .bucket("empty")
              .build()
          )
        }
      } >> IO.fromCompletableFuture {
        IO {
          cl.createBucket(
            CreateBucketRequest
              .builder()
              .bucket("nonempty")
              .build()
          )
        }
      } >> IO.fromCompletableFuture {
        IO {
          cl.putObject(
            PutObjectRequest
              .builder()
              .bucket("nonempty")
              .key("data.fdml")
              .build(),
            Paths.get(getClass().getResource("/data.fdml").toURI())
          )
        }
      } >> IO.fromCompletableFuture {
        IO {
          cl.putObject(
            PutObjectRequest
              .builder()
              .bucket("nonempty")
              .key("data2.fdml")
              .build(),
            Paths.get(getClass().getResource("/data2.fdml").toURI())
          )
        }
      }
    }

    ResourceSuiteLocalFixture("s3-client", client)
  }

  override val munitFixtures = List(s3Client)

  test("empty") {
    IO(s3Client()).flatTap { client =>
      S3FdmlCatalog
        .fromS3Client(client, "empty")
        .flatMap(_.datasets.compile.count)
        .assertEquals(0L)
    }
  }

  test("nonempty") {
    IO(s3Client()).flatTap { client =>
      S3FdmlCatalog
        .fromS3Client(client, "nonempty")
        .flatMap(_.datasets.compile.count)
        .assertEquals(2L)
    }
  }
}
