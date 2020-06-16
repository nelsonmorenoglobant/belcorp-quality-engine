name := "belcorp-quality-engine"

version := "0.1"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "jitpack.io" at "https://jitpack.io"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "com.amazon.deequ" % "deequ" % "1.0.2",
  "com.typesafe.play" %% "play-json" % "2.7.4",
  "biz.belcorp" % "dl" % "1.0" from "file:///Users/nelsonmanuelmorenorestrepo/IdeaProjects/BelcorpUtils/target/scala-2.11/belcorputils_2.11-0.1.jar ",
  "com.amazonaws" % "aws-java-sdk-elasticsearch" % "1.11.307",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.307",
  "com.amazonaws" % "aws-java-sdk-glue" % "1.11.307",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3" % "provided",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.rogach" %% "scallop" % "3.3.1",
  "com.typesafe" % "config" % "1.3.3",
  "com.github.awslabs" % "aws-request-signing-apache-interceptor" % "deb7941",
  "org.elasticsearch" % "elasticsearch" % "7.5.0",
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.5.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.10.0"
)
