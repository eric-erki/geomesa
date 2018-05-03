/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.quadtree.Quadtree

import scala.collection.JavaConverters._

/**
 * Spatial index wrapper for un-synchronized quad tree
 */
@deprecated
class WrappedQuadtree[T] extends SpatialIndex[T] with Serializable {

  private var qt = new Quadtree

  override def insert(x: Double, y: Double, key: String, item: T): Unit =
    qt.insert(new Envelope(x, x, y, y), (key, item))

  override def remove(x: Double, y: Double, key: String): T = {
    val env = new Envelope(x, x, y, y)
    qt.query(env).asScala.asInstanceOf[Seq[(String, T)]].find(_._1 == key) match {
      case None => null.asInstanceOf[T]
      case Some(kv) => qt.remove(env, kv); kv._2
    }
  }

  override def get(x: Double, y: Double, key: String): T = {
    val env = new Envelope(x, x, y, y)
    qt.query(env).asScala.asInstanceOf[Seq[(String, T)]].find(_._1 == key) .map(_._2).getOrElse(null.asInstanceOf[T])
  }

  override def query(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Iterator[T] =
    qt.query(new Envelope(xmin, xmax, ymin, ymax)).iterator().asScala.asInstanceOf[Iterator[(String, T)]].map(_._2)

  override def size(): Int = qt.size()

  override def clear(): Unit = qt = new Quadtree
}
