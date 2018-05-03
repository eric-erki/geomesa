/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.index

import org.geotools.data.{FeatureReader, FilteringFeatureReader}
import org.geotools.data.simple.{DelegateSimpleFeatureReader, FilteringSimpleFeatureReader, SimpleFeatureReader}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.spatial.BinarySpatialOperator

trait SpatialIndexSupport {

  import scala.collection.JavaConversions._

  def sft: SimpleFeatureType

  def spatialIndex: SpatialIndex[SimpleFeature]

  def allFeatures(): Iterator[SimpleFeature]

  def getReaderForFilter(filter: Filter): FeatureReader[SimpleFeatureType, SimpleFeature] = filter match {
    case f: IncludeFilter => include()
    case f: BinarySpatialOperator => spatial(f)
    case f: And => and(f)
    case f: Or => or(f)
    case f => unoptimized(f)
  }

  def include(): FeatureReader[SimpleFeatureType, SimpleFeature] = reader(allFeatures())

  def spatial(f: BinarySpatialOperator): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val geometries = FilterHelper.extractGeometries(f, sft.getGeomField, intersect = false)
    if (geometries.isEmpty) {
      unoptimized(f)
    } else {
      val env = geometries.values.head.getEnvelopeInternal
      geometries.values.tail.foreach(g => env.expandToInclude(g.getEnvelopeInternal))
      reader(spatialIndex.query(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY, f.evaluate))
    }
  }

  def and(a: And): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val geometries = extractGeometries(a, sft.getGeometryDescriptor.getLocalName, intersect = false)
    if (geometries.isEmpty) {
      unoptimized(a)
    } else {
      val env = geometries.values.head.getEnvelopeInternal
      geometries.values.tail.foreach(g => env.expandToInclude(g.getEnvelopeInternal))
      reader(spatialIndex.query(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY, a.evaluate))
    }
  }

  def or(o: Or): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val readers = o.getChildren.map(getReaderForFilter).map(SelfClosingIterator.apply(_))
    val composed = readers.foldLeft(CloseableIterator.empty[SimpleFeature])(_ ++ _)
    reader(composed)
  }

  def unoptimized(f: Filter): FeatureReader[SimpleFeatureType, SimpleFeature] = new FilteringFeatureReader(include(), f)

  protected def reader(iter: Iterator[SimpleFeature]): FeatureReader[SimpleFeatureType, SimpleFeature] =
    new DelegateSimpleFeatureReader(sft, new DelegateSimpleFeatureIterator(iter))
}
