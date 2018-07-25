/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.hbase

import java.util.concurrent.ExecutorService

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.security.User
import org.locationtech.geomesa.hbase.data.{HBaseConnectionPool, HBaseDataStoreParams}
import org.slf4j.LoggerFactory

class GeoMesaHBaseConnection(conf: Configuration, managed: Boolean, es: ExecutorService, user: User) extends Connection {
  import scala.collection.JavaConversions._

  private val log = LoggerFactory.getLogger(classOf[GeoMesaHBaseConnection])

  private val localConf = {
    val zk = conf.get(HConstants.ZOOKEEPER_QUORUM)
    Map(HBaseDataStoreParams.ZookeeperParam.key -> zk)
  }
  private val conn = HBaseConnectionPool.getConnection(localConf, false)
  
  override def getConfiguration: Configuration = conn.getConfiguration

  override def getTable(tableName: TableName): Table = conn.getTable(tableName)

  override def getTable(tableName: TableName, pool: ExecutorService): Table = conn.getTable(tableName)

  override def getBufferedMutator(tableName: TableName): BufferedMutator = conn.getBufferedMutator(tableName)

  override def getBufferedMutator(params: BufferedMutatorParams): BufferedMutator = conn.getBufferedMutator(params)

  override def getRegionLocator(tableName: TableName): RegionLocator = conn.getRegionLocator(tableName)

  override def getAdmin: Admin = conn.getAdmin

  // NOOP
  override def close(): Unit = {
    log.info("Not closing connection")
  }

  override def isClosed: Boolean = conn.isClosed

  override def abort(why: String, e: Throwable): Unit = {
    log.info("Aborting connection")
    conn.abort(why, e)
  }

  override def isAborted: Boolean = conn.isAborted
}
