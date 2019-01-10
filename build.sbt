ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.11.12"

val fs2Version    = "1.0.2"
val http4sVersion = "0.20.0-M4"

lazy val commonSettings = compilerFlags ++ Seq(
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-core"   % "1.5.0",
    "org.typelevel" %% "cats-effect" % "1.1.0",
    "co.fs2"        %% "fs2-core"    % fs2Version,
    "co.fs2"        %% "fs2-io"      % fs2Version
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

//=== Sub-projects ============================================================

lazy val core = project
  .settings(commonSettings)
  .settings(
    name := "latis3-core",
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-xml"       % "1.0.6",
      "junit"                   % "junit"           % "4.12" % Test,
      "com.novocode"            % "junit-interface" % "0.11" % Test
    )
  )

lazy val `dap2-service` = project
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.http4s"     %% "http4s-core" % http4sVersion,
      "org.http4s"     %% "http4s-dsl"  % http4sVersion,
      "org.tpolecat"   %% "atto-core"   % "0.6.3",
      "org.scalacheck" %% "scalacheck"  % "1.13.5" % Test,
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.8" % Test
    )
  )
