ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"

ThisBuild / assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case x => MergeStrategy.first
}

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    "org.scala-lang.modules" %% "scala-async" % "1.0.1",
    "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided,
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
    "ch.qos.logback" % "logback-classic" % "1.3.11", // Must use 1.3.X to run properly on JDK 8
    "org.scalatest" %% "scalatest" % "3.2.17" % "test"
  ),
  scalacOptions ++= Seq(
    "-Xasync",
    "-unchecked", "-deprecation", "-feature"
  )
)

lazy val utils = (project in file("utils"))
  .settings(commonSettings)
  .settings(
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    )
  )

lazy val master = (project in file("master"))
  .settings(commonSettings)
  .settings(
    assembly / assemblyJarName := "master.jar",
    assembly / assemblyOutputPath := file("master.jar")
  )
  .dependsOn(utils)

lazy val worker = (project in file("worker"))
  .settings(commonSettings)
  .settings(
    assembly / assemblyJarName := "worker.jar",
    assembly / assemblyOutputPath := file("worker.jar")
  )
  .dependsOn(utils)

lazy val root = (project in file("."))
  .aggregate(utils, master, worker)

Compile / run / mainClass := Some("FileTransferClient.scala")
