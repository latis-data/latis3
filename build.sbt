ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "3.3.5"

val attoVersion       = "0.9.5"
val catsVersion       = "2.13.0"
val catsEffectVersion = "3.6.1"
val fs2Version        = "3.12.0"
val http4sVersion     = "0.23.30"
val log4catsVersion   = "2.7.0"
val log4jVersion      = "2.24.3"
val logbackVersion    = "1.3.15"
val netcdfVersion     = "5.7.0"
val pureconfigVersion = "0.17.9"

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-core"   % catsVersion,
    "org.typelevel" %% "cats-effect" % catsEffectVersion,
    "co.fs2"        %% "fs2-core"    % fs2Version,
    "co.fs2"        %% "fs2-io"      % fs2Version,
    "com.typesafe"   % "config"      % "1.4.3",
    "org.scalacheck" %% "scalacheck" % "1.18.1" % Test,
    "org.scalameta" %% "munit"       % "1.1.1" % Test,
    "org.typelevel" %% "munit-cats-effect" % "2.1.0" % Test,
    "org.scalameta" %% "munit-scalacheck" % "1.1.0" % Test
  ),
  Test / fork := true,
  scalacOptions -= "-Xfatal-warnings",
  scalacOptions += {
    if (insideCI.value) "-Wconf:any:e" else "-Wconf:any:w"
  },
  Test / scalacOptions -= "-Wnonunit-statement"
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
      from("eclipse-temurin:21-alpine")
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
      "com.amazonaws"             % "aws-lambda-java-core"   % "1.2.3",
      "com.amazonaws"             % "aws-lambda-java-events" % "3.11.4",
      "com.amazonaws"             % "aws-lambda-java-log4j2" % "1.6.0" % Runtime,
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
      "org.scala-lang.modules" %% "scala-xml"           % "2.3.0",
      "io.circe"               %% "circe-core"          % "0.14.13",
      "org.scodec"             %% "scodec-cats"         % "1.2.0",
      "org.scodec"             %% "scodec-core"         % "2.3.2",
      "org.scodec"             %% "scodec-stream"       % "3.0.2",
      "org.http4s"             %% "http4s-ember-client" % http4sVersion,
      "org.gnieh"              %% "fs2-data-csv"        % "1.8.1"
    )
  )

lazy val example = project
  .dependsOn(server)
  .settings(
    name := "latis3-example",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion % Runtime
    ),
    reStart / mainClass := Some("latis.server.Latis3Server")
  )

lazy val `fdml-validator` = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "fdml-validator"
  )

lazy val ftp = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "latis3-ftp",
    libraryDependencies ++= Seq(
      "com.github.regis-leray" %% "fs2-ftp" % "0.8.6"
    )
  )

lazy val `dap2-parser` = project
  .dependsOn(macros)
  .settings(commonSettings)
  .settings(
    name := "dap2-parser",
    libraryDependencies ++= Seq(
      "org.tpolecat"   %% "atto-core"  % attoVersion
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
      "org.http4s"     %% "http4s-core"      % http4sVersion % Provided,
      "org.http4s"     %% "http4s-dsl"       % http4sVersion % Provided,
      "org.http4s"     %% "http4s-circe"     % http4sVersion,
      "org.http4s"     %% "http4s-scalatags" % "0.25.2",
      "org.typelevel"  %% "paiges-cats"      % "0.4.4"
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
      "org.http4s"            %% "http4s-ember-server"    % http4sVersion,
      "org.http4s"            %% "http4s-core"            % http4sVersion,
      "org.http4s"            %% "http4s-dsl"             % http4sVersion,
      "com.github.pureconfig" %% "pureconfig-core"        % pureconfigVersion,
      "com.github.pureconfig" %% "pureconfig-cats-effect" % pureconfigVersion,
      "com.github.pureconfig" %% "pureconfig-ip4s"        % pureconfigVersion,
      "org.typelevel"         %% "log4cats-slf4j"         % log4catsVersion,
      "ch.qos.logback"         % "logback-classic"        % logbackVersion % Runtime
    ),
    assemblyMergeStrategy := {
      case "META-INF/versions/9/module-info.class" => MergeStrategy.discard
      case x =>
        val old = assemblyMergeStrategy.value
        old(x)
    }
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
    scalacOptions -= "-Xfatal-warnings",
    scalacOptions += {
      if (insideCI.value) "-Wconf:any:e" else "-Wconf:any:w"
    }
  )

lazy val macros = project
  .settings(commonSettings)
  .settings(
    name := "latis3-macros",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "literally" % "1.2.0"
    )
  )

lazy val netcdf = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "latis3-netcdf",
    libraryDependencies ++= Seq(
      "edu.ucar"       % "netcdf4"          % netcdfVersion,
      "ch.qos.logback" % "logback-classic"  % logbackVersion  % Test
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
      "org.tpolecat"   %% "doobie-core" % "1.0.0-RC9",
      "com.h2database"  % "h2"          % "2.2.224" % Test
    )
  )

lazy val `test-utils` = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "latis3-test-utils",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.19"
    ),
  )
