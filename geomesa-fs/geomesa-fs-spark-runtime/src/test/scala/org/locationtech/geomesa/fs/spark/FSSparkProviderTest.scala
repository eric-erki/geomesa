/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.spark

import java.nio.file.{Files, Path}
import java.util.Collections

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.spark.sql.{DataFrame, SQLContext, SQLTypes, SparkSession}
import org.geotools.data.{Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.FileSystemDataStoreFactory
import org.locationtech.geomesa.fs.storage.common.PartitionScheme
import org.locationtech.geomesa.spark.SparkSQLTestUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FSSparkProviderTest extends Specification with BeforeAfterAll with LazyLogging {

  import org.locationtech.geomesa.filter.ff

  sequential
  lazy val sftName: String = "chicago"
  val tempDir: Path = Files.createTempDirectory("fsSparkTest")
  var cluster: MiniDFSCluster = _
  var directory: String = _

  def spec: String = SparkSQLTestUtils.ChiSpec

  def dtgField: Option[String] = Some("dtg")

  lazy val dsParams = Map(
    "fs.path" -> directory,
    "fs.encoding" -> "orc")
  lazy val dsf = new FileSystemDataStoreFactory()

  lazy val ds = dsf.createDataStore(dsParams)

  var spark: SparkSession = null
  var sc: SQLContext = null

  var df: DataFrame = null

  lazy val params = dsParams

  override def beforeAll(): Unit = {
    // Start MiniCluster
    val conf = new HdfsConfiguration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.toFile.getAbsolutePath)
    cluster = new MiniDFSCluster.Builder(conf).build()
    directory = cluster.getURI + s"/data/$sftName"
    val ds = dsf.createDataStore(dsParams)

    val sft = SparkSQLTestUtils.ChicagoSpec
    PartitionScheme.addToSft(sft, PartitionScheme(sft, "z2", Collections.singletonMap("z2-resolution", "8")))
    ds.createSchema(sft)

    SparkSQLTestUtils.ingestChicago(ds)
  }

  "FileSystem datastore Spark Data Tests" should {
    // before
    "start spaanyork" >> {
      spark = SparkSQLTestUtils.createSparkSession()
      sc = spark.sqlContext
      SQLTypes.init(sc)

      df = spark.read
        .format("geomesa")
        .options(params.map { case (k, v) => k -> v.toString })
        .option("geomesa.feature", "chicago")
        .load()

      logger.debug(df.schema.treeString)
      df.createOrReplaceTempView("chicago")
      true
    }

    "select * from chicago" >> {
      sc.sql("select * from chicago").collect().length must beEqualTo(3)
    }

    "select count(*) from chicago" >> {
      val rows = sc.sql("select count(*) from chicago").collect()
      rows.length mustEqual(1)
      rows.apply(0).get(0).asInstanceOf[Long] mustEqual(3l)
    }

    "select by secondary indexed attribute" >> {
      val cases = df.select("case_number").where("case_number = 1").collect().map(_.getInt(0))
      cases.length mustEqual 1
    }

    "complex st_buffer" >> {
      val buf = sc.sql("select st_asText(st_bufferPoint(geom,10)) from chicago where case_number = 1").collect().head.getString(0)
      sc.sql(
        s"""
           |select *
           |from chicago
           |where
           |  st_contains(st_geomFromWKT('$buf'), geom)
                        """.stripMargin
      ).collect().length must beEqualTo(1)
    }

    "write data and properly index" >> {
      val subset = sc.sql("select case_number,geom,dtg from chicago")
      subset.write.format("geomesa")
        .options(params.map { case (k, v) => k -> v.toString })
        .option("geomesa.feature", "chicago2")
        .save()

      val sft = ds.getSchema("chicago2")
      val enabledIndexes = sft.getUserData.get("geomesa.indices").asInstanceOf[String]
      enabledIndexes.indexOf("z3") must be greaterThan -1
    }.pendingUntilFixed("FSDS can't guess the parameters")

//    "handle reuse __fid__ on write if available" >> {
//
//      val subset = sc.sql("select __fid__,case_number,geom,dtg from chicago")
//      subset.write.format("geomesa")
//        .options(params.map { case (k, v) => k -> v.toString })
//        .option("geomesa.feature", "fidOnWrite")
//        .save()
//      val filter = ff.equals(ff.property("case_number"), ff.literal(1))
//      val queryOrig = new Query("chicago", filter)
//      val origResults = SelfClosingIterator(ds.getFeatureReader(queryOrig, Transaction.AUTO_COMMIT)).toList
//
//      val query = new Query("fidOnWrite", filter)
//      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
//
//      results.head.getID must be equalTo origResults.head.getID
//    }
  }

  override def afterAll(): Unit = {
    // Stop MiniCluster
    cluster.shutdown()
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
