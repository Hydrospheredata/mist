package io.hydrosphere.mist.core

import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.{Answer, OngoingStubbing}

import scala.concurrent.Future
import scala.reflect.ClassTag

trait MockitoSugar extends org.scalatest.mockito.MockitoSugar {

  def any[T <: AnyRef](implicit classTag: ClassTag[T]): T = {
    org.mockito.Matchers.any(classTag.runtimeClass.asInstanceOf[Class[T]])
  }

  def when[T](call: T): org.mockito.stubbing.OngoingStubbing[T] = {
    org.mockito.Mockito.when(call)
  }

  implicit class FutureStubbing[A](stubbing: OngoingStubbing[Future[A]]) {

    def thenSuccess(a: A): OngoingStubbing[Future[A]] =
      stubbing.thenReturn(Future.successful(a))

    def thenFailure(e: Throwable): OngoingStubbing[Future[A]] =
      stubbing.thenReturn(Future.failed(e))
  }

  implicit class RespondFuncSyntax[A](stubbing: OngoingStubbing[A]) {

    def thenRespond[A1](f: A1 => A): OngoingStubbing[A] = {
      stubbing.thenAnswer(new Answer[A] {
        def answer(invocation: InvocationOnMock): A = {
          val args = invocation.getArguments
          val a1 = args(0).asInstanceOf[A1]
          f(a1)
        }
      })
    }

    def thenRespond[A1, A2](f: (A1, A2) => A): OngoingStubbing[A] = {
      stubbing.thenAnswer(new Answer[A] {
        def answer(invocation: InvocationOnMock): A = {
          val args = invocation.getArguments
          val a1 = args(0).asInstanceOf[A1]
          val a2 = args(1).asInstanceOf[A2]
          f(a1, a2)
        }
      })
    }

    def thenRespond[A1, A2, A3](f: (A1, A2, A3) => A): OngoingStubbing[A] = {
      stubbing.thenAnswer(new Answer[A] {
        def answer(invocation: InvocationOnMock): A = {
          val args = invocation.getArguments
          val a1 = args(0).asInstanceOf[A1]
          val a2 = args(1).asInstanceOf[A2]
          val a3 = args(1).asInstanceOf[A3]
          f(a1, a2, a3)
        }
      })
    }
  }
}
