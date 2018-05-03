/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import org.geotools.data.FeatureReader
import org.geotools.data.simple.SimpleFeatureReader
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.collection.mutable

trait KafkaConsumerFeatureCache extends SpatialIndexSupport {

  def features: mutable.Map[String, SimpleFeature]

  override def allFeatures(): Iterator[SimpleFeature] = features.valuesIterator

  def size(): Int = features.size

  // optimized for filter.include
  def size(f: Filter): Int = {
    if (f == Filter.INCLUDE) {
      features.size
    } else {
      SelfClosingIterator(getReaderForFilter(f)).length
    }
  }

  override def getReaderForFilter(filter: Filter): FeatureReader[SimpleFeatureType, SimpleFeature] =
    filter match {
      case f: Id => fid(f)
      case _ => super.getReaderForFilter(filter)
    }

  def fid(ids: Id): FeatureReader[SimpleFeatureType, SimpleFeature] =
    reader(ids.getIDs.flatMap(id => features.get(id.toString)).iterator)
}

