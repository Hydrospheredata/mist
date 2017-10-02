import sbt._
import sbt.Keys._

object PublishSettings {

  val settings = Seq(
    publishMavenStyle := true,
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots/")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2/")
    },
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },

    pomExtra := <url>https://github.com/Hydrospheredata/mist</url>
      <licenses>
        <license>
          <name>Apache 2.0 License</name>
          <url>https://github.com/Hydrospheredata/mist/LICENSE</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>https://github.com/Hydrospheredata/mist.git</url>
        <connection>https://github.com/Hydrospheredata/mist.git</connection>
      </scm>
      <developers>
        <developer>
          <id>mkf-simpson</id>
          <name>Konstantin Makarychev</name>
          <url>https://github.com/mkf-simpson</url>
          <organization>Hydrosphere</organization>
          <organizationUrl>http://hydrosphere.io/</organizationUrl>
        </developer>
        <developer>
          <id>leonid133</id>
          <name>Leonid Blokhin</name>
          <url>https://github.com/leonid133</url>
          <organization>Hydrosphere</organization>
          <organizationUrl>http://hydrosphere.io/</organizationUrl>
        </developer>
        <developer>
          <id>dos65</id>
          <name>Vadim Chelyshov</name>
          <url>https://github.com/dos65</url>
          <organization>Hydrosphere</organization>
          <organizationUrl>http://hydrosphere.io/</organizationUrl>
        </developer>
        <developer>
          <id>blvp</id>
          <name>Pavel Borobov</name>
          <url>https://github.com/blvp</url>
          <organization>Hydrosphere</organization>
          <organizationUrl>http://hydrosphere.io/</organizationUrl>
        </developer>
      </developers>
  )
}
