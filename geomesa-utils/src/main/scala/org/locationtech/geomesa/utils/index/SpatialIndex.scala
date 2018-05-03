/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import com.vividsolutions.jts.geom.Envelope

/**
 * Trait for indexing and querying spatial data
 */
trait SpatialIndex[T] {

  def insert(x: Double, y: Double, key: String, item: T): Unit

  def remove(x: Double, y: Double, key: String): T

  def get(x: Double, y: Double, key: String): T

  def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[T]

  def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double, filter: (T) => Boolean): Iterator[T] =
    query(xmin, ymin, xmax, ymax).filter(filter)

  def size(): Int

  def clear(): Unit

  @deprecated("insert(x, y, key, item)")
  def insert(envelope: Envelope, item: T): Unit = {
    val (x, y) = SpatialIndex.getCenter(envelope)
    insert(x, y, item.toString, item)
  }

  @deprecated("remove(x, y, key, item)")
  def remove(envelope: Envelope, item: T): Boolean = {
    val (x, y) = SpatialIndex.getCenter(envelope)
    remove(x, y, item.toString) != null
  }

  @deprecated("query(xmin, ymin, xmax, ymax)")
  def query(envelope: Envelope): Iterator[T] =
    query(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)

  @deprecated("query(xmin, ymin, xmax, ymax, filter)")
  def query(envelope: Envelope, filter: (T) => Boolean): Iterator[T] =
    query(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY, filter)
}

object SpatialIndex {
  def getCenter(envelope: Envelope): (Double, Double) = {
    val x = (envelope.getMinX + envelope.getMaxX) / 2.0
    val y = (envelope.getMinY + envelope.getMaxY) / 2.0
    (x, y)
  }
}