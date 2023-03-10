ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "Producer"
  )

val logbackVersion = "1.2.10"
val slf4j_version = "1.7.5"
val log4j_api_version = "2.17.2"
val StormV = "2.4.0"



libraryDependencies ++= Seq("org.apache.kafka" %% "kafka" % "3.3.1",
  "org.slf4j" % "slf4j-api" % slf4j_version,
  "org.slf4j" % "slf4j-simple" % "1.6.4",
  "org.json" % "json" % "20220320",
)

assembly/assemblyJarName := "kafka-producer-1.0.jar"

assembly/assemblyMergeStrategy  := {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
        MergeStrategy.discard
      case ps @ x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}