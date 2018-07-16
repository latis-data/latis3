ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.12.6"

lazy val latis3 = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "latis3"
  )
  .aggregate(core)

lazy val commonSettings = compilerFlags ++ Seq(
  Compile / compile / wartremoverWarnings ++= Warts.allBut(
    Wart.Any,         // false positives
    Wart.Nothing,     // false positives
    Wart.Product,     // false positives
    Wart.Serializable // false positives
  ),
  // Test suite dependencies
  libraryDependencies ++= Seq(
    "junit"            % "junit"           % "4.12"      % Test,
    "com.novocode"     % "junit-interface" % "0.11"      % Test
  )
)

lazy val compilerFlags = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf-8",
    "-feature",
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
  .settings(
    name := "latis3-core",
    commonSettings
  )
