/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import com.vividsolutions.jts.geom.Envelope
import org.locationtech.geomesa.utils.geotools.GridSnap
import org.locationtech.geomesa.utils.index.SpatialIndex.PointIndex

import scala.annotation.tailrec

/**
  * Spatial index that breaks up space into discrete buckets to index points.
  * Does not support non-point inserts.
  */
class BucketIndex[T](xBuckets: Int = 360,
                     yBuckets: Int = 180,
                     extents: Envelope = new Envelope(-180.0, 180.0, -90.0, 90.0)) extends PointIndex[T] {

  private val gridSnap = new GridSnap(extents, xBuckets, yBuckets)
  // create the buckets up front to avoid having to synchronize the whole array
  private val buckets = Array.fill(xBuckets, yBuckets)(new LazySet)

  override def insert(x: Double, y: Double, item: T): Unit = {
    val i = snapX(x)
    val j = snapY(y)
    buckets(i)(j).add(item)
  }

  override def remove(x: Double, y: Double, item: T): Boolean = {
    val i = snapX(x)
    val j = snapY(y)
    buckets(i)(j).remove(item)
  }

  override def query(envelope: Envelope): Iterator[T] = {
    val mini = snapX(envelope.getMinX)
    val maxi = snapX(envelope.getMaxX)
    val minj = snapY(envelope.getMinY)
    val maxj = snapY(envelope.getMaxY)

    new BucketIterator(mini, maxi, minj, maxj)
  }

  private def snapX(x: Double): Int = {
    val i = gridSnap.i(x)
    if (i != -1) { i } else if (x < extents.getMinX) { 0 } else { xBuckets - 1 }
  }

  private def snapY(y: Double): Int = {
    val j = gridSnap.j(y)
    if (j != -1) { j } else if (y < extents.getMinY) { 0 } else { yBuckets - 1 }
  }

  class BucketIterator private [BucketIndex] (mini: Int, maxi: Int, minj: Int, maxj: Int) extends Iterator[T] {

    private var i = mini
    private var j = minj
    private var iter = buckets(i)(j).iterator()

    @tailrec
    override final def hasNext: Boolean = iter.hasNext || {
      if (i == maxi && j == maxj) { false } else {
        if (j < maxj) {
          j += 1
        } else {
          j = minj
          i += 1
        }
        iter = buckets(i)(j).iterator()
        hasNext
      }
    }

    override def next(): T = iter.next()
  }

  private class LazySet {

    // we use a ConcurrentHashSet, which gives us iterators that aren't affected by modifications to the backing set
    // we make it lazy as many buckets might not actually be used
    lazy private val set = Collections.newSetFromMap(new ConcurrentHashMap[T, java.lang.Boolean])

    def iterator(): java.util.Iterator[T] = set.iterator()

    def add(e: T): Boolean = set.add(e)

    def remove(o: scala.Any): Boolean = set.remove(o)
  }
}
