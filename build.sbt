import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

lazy val root = (project in file("."))
  .settings(
    name := "fs2-kafka-producer",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "fs2-kafka" % "3.5.1",
      "co.fs2" %% "fs2-io" % "3.10.2",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.17.0",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.0"
    ),
    assembly / test := {},
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
      ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll
    ),
    assembly / assemblyMergeStrategy := {
        case PathList("META-INF", "versions", "9", "module-info.class", _@_*) => MergeStrategy.last
        case x =>
          val oldStrategy = (assembly / assemblyMergeStrategy).value
          oldStrategy(x)
      }
  )
