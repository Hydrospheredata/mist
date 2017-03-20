package io.hydrosphere.mist.lib.spark1

trait PublishSupport extends ContextSupport {
  private var _publisher: Publisher = _

  def publisher: Publisher = _publisher

  override private[mist] def setup(conf: SetupConfiguration): Unit = {
    super.setup(conf)
    _publisher = Publisher.create(conf.publisherConnectionString, conf.context)
  }
}
