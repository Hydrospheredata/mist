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

    licenses := Seq("Apache 2.0 License" -> url("https://github.com/Hydrospheredata/mist")),
    homepage := Some(url("https://github.com/Hydrospheredata/mist")),
    scmInfo := Some(
          ScmInfo(
            url("https://github.com/Hydrospheredata/mist"),
            "scm:git@github.com:Hydrospheredata/mist.git"
          )
        ),
    developers := List(
        Developer(
          id = "mkf-simpson",
          name = "Konstantin Makarychev",
          url = url("https://github.com/mkf-simpson"),
          email = "kmakarychev@provectus-it.com",
        ),
        Developer(
          id = "leonid133",
          name = "Leonid Blokhin",
          url = url("https://github.com/leonid133"),
          email = "lenin133@yandex.ru"
        ),
        Developer(
          id = "dos65",
          name = "Vadim Chelyshov",
          url = url("https://github.com/dos65"),
          email = "qtankle@gmail.com"
        ),
        Developer(
          id = "blvp",
          name = "Pavel Borobov",
          url = url("https://github.com/blvp"),
          email = "blvp.me@gmail.com"
        )
    )
  )
}
