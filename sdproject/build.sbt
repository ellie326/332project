ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.4"
enablePlugins(ScalaJSPlugin)

enablePlugins(ProtobufPlugin)
lazy val root = (project in file("."))
  .aggregate(master, worker) // master, worker 모듈을 연결
  .settings(
    name := "sdproject",   // 프로젝트 이름
    scalaVersion := "2.13.10"            // 사용할 Scala 버전
  )

lazy val commonSettings = Seq(
  scalaVersion := "2.13.10",
  libraryDependencies ++= Seq(
    "io.grpc" % "grpc-netty" % "1.65.1",
    "io.grpc" % "grpc-protobuf" % "1.65.1",
    "io.grpc" % "grpc-stub" % "1.64.0",
    "com.google.protobuf" % "protobuf-java" % "4.27.1"
  )
)

lazy val master = (project in file("master"))
  .settings(commonSettings *)
  .settings(
    name := "master",
    mainClass in Compile := Some("master.Master")
  )

lazy val worker = (project in file("worker"))
  .settings(commonSettings *)
  .settings(
    name := "worker",
    mainClass in Compile := Some("worker.Worker")
  )