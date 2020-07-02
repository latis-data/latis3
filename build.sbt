ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.12.11"

val attoVersion       = "0.8.0"
val catsVersion       = "2.1.1"
val catsEffectVersion = "2.1.3"
val coursierVersion   = "2.0.0-RC6-21"
val fs2Version        = "2.4.2"
val http4sVersion     = "0.21.6"
val junitVersion      = "4.13"
val netcdfVersion     = "5.3.3"
val pureconfigVersion = "0.13.0"

lazy val commonSettings = compilerFlags ++ Seq(
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-core"   % catsVersion,
    "org.typelevel" %% "cats-effect" % catsEffectVersion,
    "co.fs2"        %% "fs2-core"    % fs2Version,
    "co.fs2"        %% "fs2-io"      % fs2Version,
    "com.typesafe"   % "config"      % "1.4.0",
    "org.scalatest" %% "scalatest"   % "3.0.8" % Test
  )
)

lazy val compilerFlags = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf-8",
    "-feature",
    "-language:higherKinds",
    "-Ypartial-unification"
  ),
  Compile / compile / scalacOptions ++= Seq(
    "-unchecked",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard"
  )
)

lazy val dockerSettings = Seq(
  docker / imageNames := {
    Seq(ImageName(s"${organization.value}/latis3:${version.value}"))
  },
  docker / dockerfile := {
    val classpath = Seq(
      (Runtime / managedClasspath).value,
      (Runtime / internalDependencyAsJars).value
    ).flatten
    val mainclass = (Compile / packageBin / mainClass).value.getOrElse {
      sys.error("Expected exactly one main class")
    }
    val cp = classpath.files.map { x =>
      s"/app/${x.getName}"
    }.mkString(":")

    new Dockerfile {
      from("openjdk:8-jre-alpine")
      copy(classpath.files, "/app/")
      expose(8080)
      entryPoint("java", "-cp", cp, mainclass)
    }
  }
)

//=== Sub-projects ============================================================

lazy val core = project
  .settings(commonSettings)
  .settings(
    name := "latis3-core",
    libraryDependencies ++= Seq(
      "org.scala-lang"          % "scala-reflect"       % scalaVersion.value,
      "org.scala-lang.modules" %% "scala-xml"           % "1.3.0",
      "io.circe"               %% "circe-core"          % "0.13.0",
      "org.scodec"             %% "scodec-core"         % "1.11.7",
      "org.scodec"             %% "scodec-stream"       % "2.0.0",
      "org.http4s"             %% "http4s-blaze-client" % http4sVersion,
      "org.tpolecat"           %% "atto-core"           % attoVersion,
      "junit"                   % "junit"               % junitVersion  % Test
    ),
    scalacOptions += "-language:experimental.macros"
  )

lazy val `fdml-validator` = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "fdml-validator"
  )

lazy val `dap2-service` = project
  .dependsOn(core)
  .dependsOn(`service-interface`)
  .settings(commonSettings)
  .settings(
    name := "dap2-service-interface",
    libraryDependencies ++= Seq(
      "org.http4s"     %% "http4s-core" % http4sVersion % Provided,
      "org.http4s"     %% "http4s-dsl"  % http4sVersion % Provided,
      "org.tpolecat"   %% "atto-core"   % attoVersion,
      "org.scalacheck" %% "scalacheck"  % "1.14.3" % Test,
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5" % Test,
      "junit"           % "junit"       % junitVersion  % Test
    )
  )

lazy val python = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "latis3-python",
    libraryDependencies ++= Seq(
      "black.ninia" % "jep" % "3.9.0"
    )
  )

lazy val server = project
  .dependsOn(core)
  .dependsOn(`service-interface`)
  .enablePlugins(DockerPlugin)
  .settings(commonSettings)
  .settings(dockerSettings)
  .settings(
    name := "latis3-server",
    libraryDependencies ++= Seq(
      "io.get-coursier"       %% "coursier"               % coursierVersion,
      "io.get-coursier"       %% "coursier-cats-interop"  % coursierVersion,
      "org.http4s"            %% "http4s-blaze-server"    % http4sVersion,
      "org.http4s"            %% "http4s-core"            % http4sVersion,
      "org.http4s"            %% "http4s-dsl"             % http4sVersion,
      "com.github.pureconfig" %% "pureconfig"             % pureconfigVersion,
      "com.github.pureconfig" %% "pureconfig-cats-effect" % pureconfigVersion,
      "ch.qos.logback"         % "logback-classic"        % "1.2.3" % Runtime
    )
  )

lazy val `service-interface` = project
  .settings(compilerFlags)
  .settings(
    name := "latis3-service-interface",
    libraryDependencies ++= Seq(
      "org.http4s"    %% "http4s-core" % http4sVersion,
      "org.typelevel" %% "cats-core"   % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    )
  )

lazy val netcdf = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "latis3-netcdf",
    libraryDependencies ++= Seq(
      "edu.ucar"            % "cdm-core"         % netcdfVersion,
      "edu.ucar"            % "httpservices"     % netcdfVersion,
      "edu.ucar"            % "netcdf4"          % netcdfVersion,
    ),
    resolvers ++= Seq(
      "Unidata" at "https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"
    )
  )
