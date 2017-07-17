package io.hydrosphere.mist.master.data

import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

trait RwLock {

  private val rwLock = new ReentrantReadWriteLock()

  protected def withLock[T](l: Lock, f: => T): T = {
    l.lock()
    try {
      f
    } finally {
      l.unlock()
    }
  }

  protected def withReadLock[T](f: => T): T = withLock(rwLock.readLock, f)
  protected def withWriteLock[T](f: => T): T = withLock(rwLock.writeLock, f)
}
