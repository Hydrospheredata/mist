package io.hydrosphere.mist.master

import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.master.data.ContextsStorage
import io.hydrosphere.mist.master.execution.ExecutionService
import io.hydrosphere.mist.master.interfaces.http.ContextCreateRequest
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import org.scalatest.{FunSpec, Matchers}
import org.mockito.Mockito._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class ContextsCrudLikeSpec extends FunSpec with Matchers with TestData with MockitoSugar {

  it("should fallback on default on creation") {
    val storage = mock[ContextsStorage]
    val executionS = mock[ExecutionService]

    val defaultValue = ContextConfig("default", Map.empty, Duration.Inf, 20, precreated = false, "", RunMode.Shared, 1 seconds, 5)
    val req = ContextCreateRequest("yoyo", workerMode = Some(RunMode.ExclusiveContext))

    when(storage.defaultConfig).thenReturn(defaultValue)
    when(storage.update(any[ContextConfig])).thenRespond[ContextConfig](ctx => Future.successful(ctx))

    val crud = new ContextsCRUDMixin {
      val execution: ExecutionService = executionS
      val contextsStorage: ContextsStorage = storage
    }

    val result = Await.result(crud.create(req), Duration.Inf)
    result shouldBe ContextConfig("yoyo", Map.empty, Duration.Inf, 20, precreated = false, "", RunMode.ExclusiveContext, 1 seconds, 5)

    verify(executionS).updateContext(any[ContextConfig])
  }
}
