ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.12.8"

val coursierVersion   = "2.0.0-RC3-2"
val fs2Version        = "1.0.2"
val http4sVersion     = "0.20.10"
val pureconfigVersion = "0.10.1"

lazy val commonSettings = compilerFlags ++ Seq(
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-core"   % "1.5.0",
    "org.typelevel" %% "cats-effect" % "1.1.0",
    "co.fs2"        %% "fs2-core"    % fs2Version,
    "co.fs2"        %% "fs2-io"      % fs2Version,
    "com.typesafe"   % "config"      % "1.3.4"
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
      "org.scala-lang.modules" %% "scala-xml"       % "1.0.6",
      "junit"                   % "junit"           % "4.12"  % Test,
      "org.scalatest"          %% "scalatest"       % "3.0.5" % Test
    )
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
      "org.tpolecat"   %% "atto-core"   % "0.6.3",
      "org.scalacheck" %% "scalacheck"  % "1.13.5" % Test,
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.8" % Test,
      "junit"           % "junit"       % "4.12"  % Test,
      "org.scalatest"  %% "scalatest"   % "3.0.5" % Test
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
      "io.get-coursier"       %% "coursier-cache"         % coursierVersion,
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
      "org.typelevel" %% "cats-core"   % "1.5.0",
      "org.typelevel" %% "cats-effect" % "1.1.0"
    )
  )
