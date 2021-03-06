ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.13.6"

val attoVersion       = "0.9.5"
val catsVersion       = "2.6.1"
val catsEffectVersion = "3.1.1"
val fs2Version        = "3.0.6"
val http4sVersion     = "0.23.0-RC1"
val log4catsVersion   = "2.1.1"
val log4jVersion      = "2.14.1"
val netcdfVersion     = "5.4.1"
val pureconfigVersion = "0.15.0"
val scalaTestVersion  = "3.2.9"

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-core"   % catsVersion,
    "org.typelevel" %% "cats-effect" % catsEffectVersion,
    "co.fs2"        %% "fs2-core"    % fs2Version,
    "co.fs2"        %% "fs2-io"      % fs2Version,
    "com.typesafe"   % "config"      % "1.4.1",
    "org.scalatest" %% "scalatest"   % scalaTestVersion % Test
  ),
  Test / fork := true,
  scalacOptions -= "-Xfatal-warnings",
  addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.0" cross CrossVersion.full)
)

lazy val dockerSettings = Seq(
  docker / imageNames := {
    Seq(ImageName(s"${organization.value}/latis3:${version.value}"))
  },
  docker / dockerfile := {
    val mainclass = (Compile / packageBin / mainClass).value.getOrElse {
      sys.error("Expected exactly one main class")
    }
    val depClasspath = (Runtime / managedClasspath).value
    val intClasspath = (Runtime / internalDependencyAsJars).value
    val cp = (depClasspath ++ intClasspath).files.map { x =>
      s"/app/${x.getName}"
    }.mkString(":")

    new Dockerfile {
      from("openjdk:8-jre-alpine")
      expose(8080)
      entryPoint("java", "-cp", cp, mainclass)
      copy(depClasspath.files, "/app/")
      copy(intClasspath.files, "/app/")
    }
  }
)

//=== Sub-projects ============================================================

lazy val `aws-lambda` = project
  .dependsOn(core)
  .enablePlugins(DockerPlugin)
  .settings(commonSettings)
  .settings(
    name := "latis3-aws-lambda",
    libraryDependencies ++= Seq(
      "com.amazonaws"             % "aws-lambda-java-core"   % "1.2.1",
      "com.amazonaws"             % "aws-lambda-java-events" % "3.9.0",
      "com.amazonaws"             % "aws-lambda-java-log4j2" % "1.2.0" % Runtime,
      "org.apache.logging.log4j"  % "log4j-core"             % log4jVersion,
      "org.apache.logging.log4j"  % "log4j-slf4j-impl"       % log4jVersion % Runtime,
      "org.typelevel"            %% "log4cats-slf4j"         % log4catsVersion
    ),
    docker / imageNames := {
      Seq(ImageName(s"${organization.value}/latis3-lambda:${version.value}"))
    },
    docker / dockerfile := {
      val depClasspath = (Runtime / managedClasspath).value
      val intClasspath = (Runtime / internalDependencyAsJars).value
      new Dockerfile {
        from("public.ecr.aws/lambda/java:8")
        copy(depClasspath.files, "/var/task/lib/")
        copy(intClasspath.files, "/var/task/lib/")
        cmd("latis.lambda.LatisLambdaHandler")
      }
    }
  )

lazy val core = project
  .dependsOn(`dap2-parser`)
  .dependsOn(macros)
  .settings(commonSettings)
  .settings(
    name := "latis3-core",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-xml"           % "1.3.0",
      "io.circe"               %% "circe-core"          % "0.14.1",
      "org.scodec"             %% "scodec-core"         % "1.11.8",
      "org.scodec"             %% "scodec-stream"       % "3.0.1",
      "org.http4s"             %% "http4s-blaze-client" % http4sVersion,
      "com.github.regis-leray" %% "fs2-ftp"             % "0.8.0"
    )
  )

lazy val `fdml-validator` = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "fdml-validator"
  )

lazy val `dap2-parser` = project
  .dependsOn(macros)
  .settings(commonSettings)
  .settings(
    name := "dap2-parser",
    libraryDependencies ++= Seq(
      "org.tpolecat"   %% "atto-core"  % attoVersion,
      "org.scalacheck" %% "scalacheck" % "1.15.4" % Test,
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.15" % "1.3.0" % Test
    )
  )

lazy val `dap2-service` = project
  .dependsOn(core)
  .dependsOn(`dap2-parser`)
  .dependsOn(netcdf)
  .dependsOn(`service-interface`)
  .settings(commonSettings)
  .settings(
    name := "dap2-service-interface",
    libraryDependencies ++= Seq(
      "org.http4s"     %% "http4s-core" % http4sVersion % Provided,
      "org.http4s"     %% "http4s-dsl"  % http4sVersion % Provided
    )
  )

lazy val python = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "latis3-python",
    libraryDependencies ++= Seq(
      "black.ninia" % "jep" % "3.9.1"
    )
  )

lazy val server = project
  .dependsOn(core)
  .dependsOn(`dap2-service`)
  .dependsOn(`service-interface`)
  .enablePlugins(DockerPlugin)
  .settings(commonSettings)
  .settings(dockerSettings)
  .settings(
    name := "latis3-server",
    libraryDependencies ++= Seq(
      "org.http4s"            %% "http4s-blaze-server"    % http4sVersion,
      "org.http4s"            %% "http4s-core"            % http4sVersion,
      "org.http4s"            %% "http4s-dsl"             % http4sVersion,
      "com.github.pureconfig" %% "pureconfig"             % pureconfigVersion,
      "com.github.pureconfig" %% "pureconfig-cats-effect" % pureconfigVersion,
      "org.typelevel"         %% "log4cats-slf4j"         % log4catsVersion,
      "ch.qos.logback"         % "logback-classic"        % "1.2.3" % Runtime
    )
  )

lazy val `service-interface` = project
  .dependsOn(core)
  .settings(
    name := "latis3-service-interface",
    libraryDependencies ++= Seq(
      "org.http4s"    %% "http4s-core" % http4sVersion,
      "org.typelevel" %% "cats-core"   % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    ),
    scalacOptions -= "-Xfatal-warnings"
  )

lazy val macros = project
  .settings(commonSettings)
  .settings(
    name := "latis3-macros",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    ),
    scalacOptions += "-language:experimental.macros"
  )

lazy val netcdf = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "latis3-netcdf",
    libraryDependencies ++= Seq(
      "edu.ucar"            % "netcdf4"          % netcdfVersion,
    ),
    resolvers ++= Seq(
      "Unidata" at "https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
    )
  )

lazy val jdbc = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "latis3-jdbc",
    libraryDependencies ++= Seq(
      "org.tpolecat"             %% "doobie-core" % "1.0.0-M5",
      "com.h2database"            % "h2"          % "1.4.200" % Test,
    )
  )

lazy val `test-utils` = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "latis3-test-utils",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ),
  )
