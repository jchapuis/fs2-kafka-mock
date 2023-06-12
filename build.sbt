val scala213 = "2.13.10"

val scala3 = "3.2.2"

scalaVersion := scala213

crossScalaVersions := Seq(scala213, scala3)

Compile / scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, _)) => Seq("-Xsource:3")
    case _            => Nil
  }
}

organization := "io.github.jchapuis"

name := "fs2-kafka-mock"

licenses := List("Apache License, Version 2.0" -> url("https://opensource.org/license/apache-2-0/"))

developers := List(
  Developer(
    "jchapuis",
    "Jonas Chapuis",
    "me@jonaschapuis.com",
    url("https://jonaschapuis.com")
  )
)

sonatypeCredentialHost := "s01.oss.sonatype.org"

sonatypeProjectHosting := Some(xerial.sbt.Sonatype.GitHubHosting("jchapuis", "fs2-kafka-mock", "me@jonaschapuis.com"))

Global / onChangedBuildSource := ReloadOnSourceChanges

versionPolicyIntention := Compatibility.None

versionScheme := Some("early-semver")

versionPolicyIgnoredInternalDependencyVersions := Some(
  "^\\d+\\.\\d+\\.\\d+\\+\\d+".r
) // Support for versions generated by sbt-dynver

libraryDependencies ++= Seq(
  "com.github.fd4s" %% "fs2-kafka" % "3.0.1",
  "org.scalameta" %% "munit" % "1.0.0-M8" % Test,
  "org.typelevel" %% "munit-cats-effect" % "2.0.0-M3" % Test,
  "org.typelevel" %% "cats-effect-testkit" % "3.5.0" % Test
)
