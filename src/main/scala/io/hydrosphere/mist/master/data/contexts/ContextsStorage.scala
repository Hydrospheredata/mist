package io.hydrosphere.mist.master.data.contexts

import java.nio.file.Path

import io.hydrosphere.mist.master.ContextsSettings
import io.hydrosphere.mist.master.data._

import ContextConfig._

class ContextsStorage(dir: Path, default: ContextConfig)
  extends FsStorage[ContextConfig](dir) { self =>

  def precreated: Seq[ContextConfig] = entries.filter(_.precreated)

  def getOrDefault(name: String): ContextConfig = entry(name).getOrElse(default)

  def withDefaults(defaults: Seq[ContextConfig]): ContextsStorage = {
    new ContextsStorage(dir, default) {
      override def entries: Seq[ContextConfig] = defaults ++ self.entries

      override def entry(name: String): Option[ContextConfig] =
        defaults.find(_.name == name).orElse(self.entry(name))
    }
  }
}

object ContextsStorage {

  def create(dir: String, contextsSettings: ContextsSettings): ContextsStorage = {
    val defaults = contextsSettings.default +: contextsSettings.contexts.values.toList
    create(dir, contextsSettings.default).withDefaults(defaults)
  }

  def create(dir: String, default: ContextConfig): ContextsStorage =
    new ContextsStorage(checkDirectory(dir), default)

}
