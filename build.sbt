name := "simplemap-spark-scala"

version := "1.0"

scalaVersion := "2.10.6"

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

mainClass in assembly := Some("dataflows.spark.SimpleMap")

resolvers ++= Seq(
  "gkthiruvathukal@bintray" at "http://dl.bintray.com/gkthiruvathukal/maven",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
  "edu.luc.cs" %% "blockperf" % "0.4.3",
  "com.novocode" % "junit-interface" % "latest.release" % Test,
  "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
  "com.github.scopt" %% "scopt" % "3.4.0",
  "org.scalatest" %% "scalatest" % "2.2.6" % Test,
  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalanlp" %% "breeze-natives" % "0.12",
  "org.scalanlp" %% "breeze-viz" % "0.12",
  "org.json4s" %% "json4s-native" % "3.3.0"
)
