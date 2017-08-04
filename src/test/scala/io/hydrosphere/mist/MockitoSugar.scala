package io.hydrosphere.mist

import reflect.ClassTag

trait MockitoSugar extends org.scalatest.mockito.MockitoSugar {

  def any[T <: AnyRef](implicit classTag: ClassTag[T]): T = {
    org.mockito.Matchers.any(classTag.runtimeClass.asInstanceOf[Class[T]])
  }

  def when[T](call: T): org.mockito.stubbing.OngoingStubbing[T] = {
    org.mockito.Mockito.when(call)
  }
}
