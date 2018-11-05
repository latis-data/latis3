ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.11.8"

lazy val commonSettings = compilerFlags ++ Seq(
  // Test suite dependencies
  libraryDependencies ++= Seq(
    "co.fs2"          %% "fs2-core"        % "1.0.0",
    "co.fs2"          %% "fs2-io"          % "1.0.0",
    "junit"            % "junit"           % "4.12"      % Test,
    "com.novocode"     % "junit-interface" % "0.11"      % Test
  )
)

lazy val compilerFlags = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf-8",
    "-feature",
    "-language:higherKinds"
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
    name := "latis3-core"
  )
