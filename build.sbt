name := "SnowparkDemo"

version := "0.1"

scalaVersion := "2.12.13"
libraryDependencies ++= Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models"
)
resolvers += "Sonatype OSS Snapshots snowflake" at "https://oss.sonatype.org/content/repositories/comsnowflake-1049"

resolvers += "OSGeo Release Repository" at "https://repo.osgeo.org/repository/release/"

// Reminder: After the Snowpark library is released on Maven,
// you will need to update the version of the library specified below.
libraryDependencies += "com.snowflake" % "snowpark" % "0.6.0"

// If you are using a REPL, uncomment the following settings.
// Compile/console/scalacOptions += "-Yrepl-class-based"
// Compile/console/scalacOptions += "-Yrepl-outdir"
// Compile/console/scalacOptions += "repl_classes"
