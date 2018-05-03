/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.io.Closeable
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{DFI, DFR, FR}
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration

trait LiveFeatureCache extends Closeable {
  def createOrUpdateFeature(update: CreateOrUpdate): Unit
  def removeFeature(toDelete: Delete): Unit
  def clear(): Unit
  def size(): Int
  def size(filter: Filter): Int
  def getFeatureById(id: String): SimpleFeature
  def getReaderForFilter(filter: Filter): FR
}

object LiveFeatureCache {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def apply(sft: SimpleFeatureType, expiry: Duration, index: SpatialIndex[SimpleFeature]): LiveFeatureCache = {
    if (sft.isPoints) {
      new KafkaFeatureCachePoints(sft, index, expiry)
    } else {
      new KafkaFeatureCacheCentroids(sft, index, expiry)
    }
  }

  class KafkaFeatureCachePoints(val sft: SimpleFeatureType, index: SpatialIndex[SimpleFeature], expiry: Duration)
      extends LiveFeatureCache with SpatialIndexSupport with LazyLogging {

    private val executor = {
      val ex = new ScheduledThreadPoolExecutor(2)
      ex.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
      ex.setRemoveOnCancelPolicy(true)
      ex
    }

    protected val state = new ConcurrentHashMap[String, FeatureState]

    override def spatialIndex: SpatialIndex[SimpleFeature] = index

    /**
      * WARNING: this method is not thread-safe
      *
      * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
      */
    override def createOrUpdateFeature(update: CreateOrUpdate): Unit = {
      val feature = update.feature
      val pt = feature.getDefaultGeometry.asInstanceOf[Point]
      val featureState = new FeatureState(feature.getID, pt.getX, pt.getY)
      logger.trace(s"${feature.getID} inserting with expiry $expiry")
      val old = state.put(featureState.id, featureState)
      if (old != null) {
        logger.trace(s"${feature.getID} removing old feature")
        if (old.expire != null) {
          val res = old.expire.cancel(true)
          logger.trace(s"${feature.getID} cancelled expiration: $res")
        }
        index.remove(old.x, old.y, old.id)
      }
      index.insert(featureState.x, featureState.y, featureState.id, feature)
      logger.trace(s"Current index size: ${state.size()}/${index.size()}")
    }

    /**
      * WARNING: this method is not thread-safe
      *
      * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
      */
    override def removeFeature(toDelete: Delete): Unit = {
      val id = toDelete.id
      logger.trace(s"$id removing feature")
      val old = state.remove(id)
      if (old != null) {
        if (old.expire != null) {
          val res = old.expire.cancel(true)
          logger.trace(s"$id cancelled expiration: $res")
        }
        index.remove(old.x, old.y, old.id)
      }
    }

    override def clear(): Unit = {
      state.clear()
      index.clear()
    }

    override def size(): Int = state.size()

    // optimized for filter.include
    override def size(f: Filter): Int =
      if (f == Filter.INCLUDE) { size() } else { SelfClosingIterator(getReaderForFilter(f)).length}

    override def allFeatures(): Iterator[SimpleFeature] = index.query(-180.0, -90.0, 180.0, 90.0)

    override def getFeatureById(id: String): SimpleFeature =
      Option(state.get(id)).flatMap(f => Option(index.get(f.x, f.y, id))).orNull

    override def unoptimized(f: Filter): FR = {
      import scala.collection.JavaConversions._
      index match {
        case e: GeoCQEngine => e.getReaderForFilter(f)
        case _ => new DFR(sft, new DFI(index.query(-180.0, -90.0, 180.0, 90.0, f.evaluate)))
      }
    }

    override def close(): Unit = executor.shutdown()

    /**
      * Holder for our key feature values
      *
      * @param id feature id
      * @param x x coord
      * @param y y coord
      */
    protected class FeatureState(val id: String, val x: Double, val y: Double) extends Runnable {
      val expire: ScheduledFuture[_] =
        if (expiry == Duration.Inf) { null } else { executor.schedule(this, expiry.toMillis, TimeUnit.MILLISECONDS) }

      override def run(): Unit = {
        logger.trace(s"$id expiring from cache")
        state.remove(id)
        index.remove(x, y, id)
        logger.trace(s"Current index size: ${state.size()}/${index.size()}")
      }
    }
  }

  class KafkaFeatureCacheCentroids(sft: SimpleFeatureType, spatialIndex: SpatialIndex[SimpleFeature], expiry: Duration)
      extends KafkaFeatureCachePoints(sft, spatialIndex, expiry) {

    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry

    override def createOrUpdateFeature(update: CreateOrUpdate): Unit = {
      val feature = update.feature
      val pt = feature.getDefaultGeometry.asInstanceOf[Geometry].safeCentroid()
      val featureState = new FeatureState(feature.getID, pt.getX, pt.getY)
      val old = state.put(featureState.id, featureState)
      if (old != null) {
        if (old.expire != null) {
          old.expire.cancel(false)
        }
        spatialIndex.remove(old.x, old.y, old.id)
      }
      spatialIndex.insert(featureState.x, featureState.y, featureState.id, feature)
    }
  }
}