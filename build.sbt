scalaVersion := "2.12.20"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "3.5.7",
	"org.apache.spark" %% "spark-sql"  % "3.5.7",
	"ch.qos.logback" % "logback-classic" % "1.4.14",
	"org.apache.hadoop" % "hadoop-client" % "3.3.6" % "provided",
	"org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

assembly / mainClass := Some("ItemRanking")
Compile / discoveredMainClasses := Seq()

assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => xs match {
    case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
    case ("services" :: _)      => MergeStrategy.concat
    case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) => MergeStrategy.concat
    case _ => MergeStrategy.discard
  }

  case PathList("module-info.class") => MergeStrategy.discard
  case PathList("META-INF", "versions", _ @_*) => MergeStrategy.first

  case PathList("google", "protobuf", xs @ _*) => MergeStrategy.first

  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first

  case PathList("META-INF", "org", "apache", "logging", "log4j", "core", "config", "plugins", "Log4j2Plugins.dat") =>
    MergeStrategy.concat

  case _ => MergeStrategy.first
}