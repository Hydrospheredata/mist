package io.hydrosphere.mist.lib.spark1

trait Publisher extends ContextSupport {

  private var _publisher: GlobalPublisher = _

  def publisher: GlobalPublisher = _publisher

  override private[mist] def setup(conf: SetupConfiguration): Unit = {
    super.setup(conf)
    _publisher =
      GlobalPublisher.create(
        conf.publisherConnectionString,
        conf.publisherTopic,
        conf.context)
  }
}
