package io.hydrosphere.mist.utils.SparkCollections

import org.apache.spark.util.collection.OpenHashSet

import scala.reflect.ClassTag

/**
  * Created by Bulat on 20.02.2017.
  */
class OpenHashMap[K : ClassTag, @specialized(Long, Int, Double) V: ClassTag](initialCapacity: Int)
  extends Iterable[(K, V)]
    with Serializable {

  def this() = this(64)

  protected var _keySet = new OpenHashSet[K](initialCapacity)

  // Init in constructor (instead of in declaration) to work around a Scala compiler specialization
  // bug that would generate two arrays (one for Object and one for specialized T).
  private var _values: Array[V] = _
  _values = new Array[V](_keySet.capacity)

  @transient private var _oldValues: Array[V] = null

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  private var haveNullValue = false
  private var nullValue: V = null.asInstanceOf[V]

  override def size: Int = if (haveNullValue) _keySet.size + 1 else _keySet.size

  /** Tests whether this map contains a binding for a key. */
  def contains(k: K): Boolean = {
    if (k == null) {
      haveNullValue
    } else {
      _keySet.getPos(k) != OpenHashSet.INVALID_POS
    }
  }

  /** Get the value for a given key */
  def apply(k: K): V = {
    if (k == null) {
      nullValue
    } else {
      val pos = _keySet.getPos(k)
      if (pos < 0) {
        null.asInstanceOf[V]
      } else {
        _values(pos)
      }
    }
  }

  /** Set the value for a key */
  def update(k: K, v: V) {
    if (k == null) {
      haveNullValue = true
      nullValue = v
    } else {
      val pos = _keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK
      _values(pos) = v
      _keySet.rehashIfNeeded(k, grow, move)
      _oldValues = null
    }
  }

  /**
    * If the key doesn't exist yet in the hash map, set its value to defaultValue; otherwise,
    * set its value to mergeValue(oldValue).
    *
    * @return the newly updated value.
    */
  def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V = {
    if (k == null) {
      if (haveNullValue) {
        nullValue = mergeValue(nullValue)
      } else {
        haveNullValue = true
        nullValue = defaultValue
      }
      nullValue
    } else {
      val pos = _keySet.addWithoutResize(k)
      if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
        val newValue = defaultValue
        _values(pos & OpenHashSet.POSITION_MASK) = newValue
        _keySet.rehashIfNeeded(k, grow, move)
        newValue
      } else {
        _values(pos) = mergeValue(_values(pos))
        _values(pos)
      }
    }
  }

  override def iterator: Iterator[(K, V)] = new Iterator[(K, V)] {
    var pos = -1
    var nextPair: (K, V) = computeNextPair()

    /** Get the next value we should return from next(), or null if we're finished iterating */
    def computeNextPair(): (K, V) = {
      if (pos == -1) {    // Treat position -1 as looking at the null value
        if (haveNullValue) {
          pos += 1
          return (null.asInstanceOf[K], nullValue)
        }
        pos += 1
      }
      pos = _keySet.nextPos(pos)
      if (pos >= 0) {
        val ret = (_keySet.getValue(pos), _values(pos))
        pos += 1
        ret
      } else {
        null
      }
    }

    def hasNext: Boolean = nextPair != null

    def next(): (K, V) = {
      val pair = nextPair
      nextPair = computeNextPair()
      pair
    }
  }

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the non-specialized one and needs access
  // to the "private" variables).
  // They also should have been val's. We use var's because there is a Scala compiler bug that
  // would throw illegal access error at runtime if they are declared as val's.
  protected var grow = (newCapacity: Int) => {
    _oldValues = _values
    _values = new Array[V](newCapacity)
  }

  protected var move = (oldPos: Int, newPos: Int) => {
    _values(newPos) = _oldValues(oldPos)
  }
}