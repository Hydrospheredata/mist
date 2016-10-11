package io.hydrosphere.mist.lib

import io.hydrosphere.mist.contexts.ContextWrapper
import org.apache.spark.streaming._

trait SparkStreamingSupport extends ContextSupport {
  private var _streamingContext: StreamingContext = null

  protected def streamingContext: StreamingContext = _streamingContext

  override private[mist] def setup(sc: ContextWrapper): Unit = {
    super.setup(sc)
    //TODO checkpointDirectory from config
    //TODO param streaming context from config
    _streamingContext = StreamingContext.getOrCreate("checkpointDirectory/", () => {new StreamingContext(context, Seconds(1))})
  }
}
