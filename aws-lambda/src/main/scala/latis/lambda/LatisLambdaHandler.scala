package latis.lambda

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._
import feral.lambda.IOLambda
import feral.lambda.events.ApiGatewayProxyEventV2
import feral.lambda.events.ApiGatewayProxyStructuredResultV2
import feral.lambda.http4s.ApiGatewayProxyHandler
// import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.s3.S3AsyncClient

import latis.catalog.Catalog
import latis.catalog.S3FdmlCatalog
import latis.service.dap2.Dap2Service

final class LatisLambdaHandler
    extends IOLambda[ApiGatewayProxyEventV2, ApiGatewayProxyStructuredResultV2] {

  override def handler = Resource.eval(catalog).map { catalog =>
    { implicit env =>
      ApiGatewayProxyHandler(new Dap2Service(catalog).routes)
    }
  }

  private def catalog: IO[Catalog] =
    (s3Client, fdmlBucketName).flatMapN(S3FdmlCatalog.fromS3Client)

  private def fdmlBucketName: IO[String] =
    IO(Option(System.getenv("LATIS_FDML_BUCKET")).getOrElse("fdml"))

  private def s3Client: IO[S3AsyncClient] = IO {
    // TODO: See https://aws.amazon.com/blogs/developer/tuning-the-aws-java-sdk-2-x-to-reduce-startup-time
    //
    // - Use the EnvironmentVariableCredentialProvider for credential
    // lookups
    // - Set the region from the AWS_REGION environment variable
    S3AsyncClient.builder().build()
  }
}
