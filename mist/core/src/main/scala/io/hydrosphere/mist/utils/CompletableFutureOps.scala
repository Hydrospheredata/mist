package io.hydrosphere.mist.utils

import java.util.concurrent.CompletableFuture
import java.util.function.{BiConsumer, BiFunction}

import scala.concurrent.{Future, Promise}

final class CompletableFutureOps[A](val cf: CompletableFuture[A]) extends AnyVal {

  def toFuture: Future[A] = {
    val p = Promise[A]
    cf.whenComplete(new BiConsumer[A, Throwable] {
      override def accept(res: A, err: Throwable): Unit = {
        (Option(res), Option(err)) match {
        case (Some(r), None) => p.success(r)
        case (_, Some(e)) =>
          e match {
            case ce: java.util.concurrent.CompletionException => p.failure(ce.getCause)
            case _ => p.failure(e)
          }
        case (_, _) => p.failure(new IllegalStateException("CompletableFuture was failed without error information"))
      }
    }})
    p.future
  }
}

object jFutureSyntax {
  implicit def cfSyntax[A](cf: CompletableFuture[A]): CompletableFutureOps[A] = new CompletableFutureOps(cf)
}
