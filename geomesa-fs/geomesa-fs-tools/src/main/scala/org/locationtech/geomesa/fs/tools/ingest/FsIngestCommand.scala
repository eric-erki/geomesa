/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.File
import java.net.URL

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{EncodingParam, FsParams, SchemeParams}
import org.locationtech.geomesa.fs.tools.data.FsCreateSchemaCommand
import org.locationtech.geomesa.fs.tools.ingest.FileSystemConverterJob.{OrcConverterJob, ParquetConverterJob}
import org.locationtech.geomesa.fs.tools.ingest.FsIngestCommand.{FileSystemConverterIngest, FsIngestParams}
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.AbstractIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.utils.geotools.GeneralShapefileIngest
import org.locationtech.geomesa.utils.io.PathUtils
import org.locationtech.geomesa.utils.text.TextTools
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.parallel.ForkJoinTaskSupport

// TODO we need multi threaded ingest for this
class FsIngestCommand extends IngestCommand[FileSystemDataStore] with FsDataStoreCommand {

  override val params = new FsIngestParams

  override val libjarsFile: String = "org/locationtech/geomesa/fs/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_FS_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[FileSystemDataStore])
  )

  override def execute(): Unit = {
    // validate arguments
    if(params.fmt == DataFormats.Shp) {
      super.execute()
    } else {
      if (params.config == null) {
        throw new ParameterException("Converter config argument is required")
      }
      if (params.spec == null) {
        throw new ParameterException("SimpleFeatureType specification argument is required")
      }
      super.execute()
    }
  }


  override protected def createShpIngest(): Runnable = {
    new FsShapefileIngest(connection, Option(params.featureName), params.files, params)
  }

  override protected def createConverterIngest(sft: SimpleFeatureType, converterConfig: Config): Runnable = {
    FsCreateSchemaCommand.setOptions(sft, params)
    new FileSystemConverterIngest(sft,
        connection,
        converterConfig,
        params.files,
        Option(params.mode),
        params.encoding,
        libjarsFile,
        libjarsPaths,
        params.threads,
        Option(params.tempDir).map(new Path(_)),
        Option(params.reducers))
  }
}

class FsShapefileIngest(connection: java.util.Map[String, String],
                        typeName: Option[String],
                        files: Seq[String],
                        params: FsIngestParams) extends Runnable {
  override def run(): Unit = {
    import TextTools._
    Command.user.info(s"Ingesting ${getPlural(files.length, "file")}")

    val start = System.currentTimeMillis()

    // If someone is ingesting file from hdfs, S3, or wasb we add the Hadoop URL Factories to the JVM.
    if (files.exists(PathUtils.isRemote)) {
      import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
      val factory = new FsUrlStreamHandlerFactory
      URL.setURLStreamHandlerFactory(factory)
    }

    val ds = DataStoreFinder.getDataStore(connection)

    val (ingested, failed) = try {
      files.map { f =>
        if(!ds.getTypeNames.contains(typeName)) createSchema(ds, f)
        GeneralShapefileIngest.ingestToDataStore(f, ds, typeName)
      }.reduce((l: (Long, Long), r: (Long, Long)) => (l._1 + r._1, l._2 + r._2))
    } finally {
      ds.dispose()
    }
    Command.user.info(s"Shapefile ingestion complete in ${TextTools.getTime(start)}")
    Command.user.info(AbstractIngest.getStatInfo(ingested, failed))
  }

  private def createSchema(ds: DataStore, file: String): Unit = {
    // We have to modify the user data in the SimpleFeatureType, adding the FSDS options
    val shapefile = GeneralShapefileIngest.getShapefileDatastore(file)
    val schema = shapefile.getFeatureSource.getSchema
    Command.user.info(s"Creating new FSDS schema ${schema.getTypeName}")
    // Set the options
    FsCreateSchemaCommand.setOptions(schema, params)
    // Create the schema
    ds.createSchema(schema)
  }
}

object FsIngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class FsIngestParams extends IngestParams with FsParams with EncodingParam with SchemeParams with TempDirParam {
    @Parameter(names = Array("--num-reducers"), description = "Num reducers (required for distributed ingest)", required = false)
    var reducers: java.lang.Integer = _
  }

  trait TempDirParam {
    @Parameter(names = Array("--temp-path"), description = "Path to temp dir for writing output. " +
        "Note that this may be useful when using s3 since it is slow as a sink", required = false)
    var tempDir: String = _
  }

  class FileSystemConverterIngest(sft: SimpleFeatureType,
                                  dsParams: Map[String, String],
                                  converterConfig: Config,
                                  inputs: Seq[String],
                                  mode: Option[RunMode],
                                  encoding: String,
                                  libjarsFile: String,
                                  libjarsPaths: Iterator[() => Seq[File]],
                                  numLocalThreads: Int,
                                  tempPath: Option[Path],
                                  reducers: Option[java.lang.Integer])
      extends ConverterIngest(sft, dsParams, converterConfig, inputs, mode, libjarsFile, libjarsPaths, numLocalThreads) {

    override def runDistributedJob(statusCallback: StatusCallback): (Long, Long) = {
      if (reducers.isEmpty) {
        throw new ParameterException("Must provide num-reducers argument for distributed ingest")
      }
      val job = if (encoding == ParquetFileSystemStorage.ParquetEncoding) {
        new ParquetConverterJob()
      } else if (encoding == OrcFileSystemStorage.OrcEncoding) {
        new OrcConverterJob()
      } else {
        throw new ParameterException(s"Ingestion is not supported for encoding $encoding")
      }
      job.run(dsParams, sft.getTypeName, converterConfig, inputs, tempPath, reducers.get,
        libjarsFile, libjarsPaths, statusCallback)
    }
  }
}
