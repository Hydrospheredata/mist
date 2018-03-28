package io.hydrosphere.mist.worker

import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class CancellableFutureSpec extends FunSpec with Matchers {

  it("should invoke successfully") {
    val cFuture = CancellableFuture.onDetachedThread(40 + 2)
    Await.result(cFuture.future, Duration.Inf) shouldBe 42
  }

  it("should be failed") {
    val cFuture = CancellableFuture.onDetachedThread[Int](throw new RuntimeException("yoyo"))
    intercept[RuntimeException] {
      Await.result(cFuture.future, Duration.Inf)
    }
  }

  it("should be cancelled") {
    val cFuture = CancellableFuture.onDetachedThread {
      Thread.sleep(1000 * 2)
      42
    }
    intercept[RuntimeException] {
      cFuture.cancel()
      Await.result(cFuture.future, Duration.Inf)
    }
  }
}
