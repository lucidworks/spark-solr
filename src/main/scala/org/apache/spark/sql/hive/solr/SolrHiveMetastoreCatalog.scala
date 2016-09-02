package org.apache.spark.sql.hive.solr

import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreCatalog}
import org.apache.spark.sql.hive.client.ClientInterface

class SolrHiveMetastoreCatalog(
    client: ClientInterface,
    context: HiveContext,
    permissionChecker: Option[TablePermissionChecker])
  extends HiveMetastoreCatalog(client, context) {

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val tables: Seq[(String, Boolean)] = super.getTables(None)
    // Checks for ACLs on tables
    if (permissionChecker.isDefined) permissionChecker.get.checkPermissions(tables)
    tables
  }

}

trait TablePermissionChecker {

  def checkPermissions(tables: Seq[(String, Boolean)])

}
