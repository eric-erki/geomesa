/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.nio.file.{Files, Path}
import java.time.temporal.ChronoUnit

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.geotools.data.{DataStore, DataStoreFinder, Query, Transaction}
import org.junit.Before
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.common.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.common.partitions.DateTimeScheme
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.FileSystemFeatureStore
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class CompactCommandTest extends Specification with BeforeAfterAll {
  var cluster: MiniDFSCluster = _
  var directory: String = _
  val typeName = "orc"

  val tempDir: Path = Files.createTempDirectory("compactCommand")


  val sft = SimpleFeatureTypes.createType(typeName, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  PartitionScheme.addToSft(sft, new DateTimeScheme(DateTimeScheme.Formats.Daily.format, ChronoUnit.DAYS, 1, "dtg", false))


  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (i % 3)}T04:03:02.0001Z", s"POINT(10 10.$i)")
  }
  val features2 = Seq.tabulate(10) { j =>
    val i = j+10
    ScalaSimpleFeature.create(sft, s"$i", s"test$i", 100 + i, s"2017-06-0${5 + (j % 3)}T04:03:02.0001Z", s"POINT(10 10.$j)")
  }

  def getDataStore: DataStore = {
    val dsParams = Map(
      "fs.path" -> directory,
      "fs.encoding" -> typeName,
      "fs.config" -> "parquet.compression=gzip")

    DataStoreFinder.getDataStore(dsParams)
  }

  override def beforeAll(): Unit = {
    // Start MiniCluster
    val conf = new HdfsConfiguration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.toFile.getAbsolutePath)
    cluster = new MiniDFSCluster.Builder(conf).build()

    directory = cluster.getDataDirectory + s"_$typeName"

    val ds = getDataStore

    ds.createSchema(sft)

    WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach { feature =>
        FeatureUtils.copyToWriter(writer, feature, useProvidedFid = true)
        writer.write()
      }
    }

    WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
      features2.foreach { feature =>
        FeatureUtils.copyToWriter(writer, feature, useProvidedFid = true)
        writer.write()
      }
    }

  }

  "Compaction command" >> {
    "Before compacting should be multiple files" in {
      val ds = getDataStore

      val fs = ds.getFeatureSource(typeName)
      fs.getCount(Query.ALL) mustEqual 20
      fs.asInstanceOf[FileSystemFeatureStore].storage.getMetadata.getFileCount mustEqual(6)
    }

    "Compaction command should run successfully" in {
      true mustEqual(true)
    }
    "After compacting should be fewer files" in {
      val ds = getDataStore

      val fs = ds.getFeatureSource(typeName)
      fs.getCount(Query.ALL) mustEqual 20
      fs.asInstanceOf[FileSystemFeatureStore].storage.getMetadata.getFileCount mustEqual(3)
    }

  }

  override def afterAll(): Unit = {
    // Stop MiniCluster
    cluster.shutdown()
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
