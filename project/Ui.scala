import java.net.HttpURLConnection

import sbt.Keys._
import sbt._

import scala.annotation.switch


object Ui {

  lazy val uiVersion: SettingKey[String] = settingKey[String]("Ui version")
  lazy val uiUrl: SettingKey[String => String] = settingKey[String => String]("Construct url for ui downloading")
  lazy val uiCheckoutDir: SettingKey[String] = settingKey[String]("Directory for downloading ui")
  lazy val ui: TaskKey[File] = taskKey[File]("Download ui or return cached")

  lazy val settings = Seq(
    uiUrl := { (s: String) => s"https://github.com/Hydrospheredata/mist-ui/releases/download/v$s/mist-ui-$s.tar.gz" },
    uiVersion := "2.2.1",
    uiCheckoutDir := "ui_local",
    ui := {
      val local = baseDirectory.value / uiCheckoutDir.value
      if (!local.exists()) IO.createDirectory(local)

      val v = uiVersion.value
      val target = local / s"ui-$v"
      if (!target.exists()) {
        val link = uiUrl.value(v)
        val targetF = local/ s"ui-$v.tar.gz"
        Downloader.download(link, targetF)
        Tar.extractTarGz(targetF, target)
      }
      target / "dist"
    }
  )

}
