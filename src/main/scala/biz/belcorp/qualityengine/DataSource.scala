package biz.belcorp.qualityengine

import biz.belcorp.dl.utils.{Params, SparkUtils}
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.JsValue


class DataSource(spark: SparkSession) {

  def getDataFrame(interfaceName: String, objectS3: String, jsonAttributes: JsValue, params: Params, config: Config): DataFrame = {

    val databasesConfig = config.getConfig("databases")
    val tableName = s"${params.system()}_$interfaceName"
    val landingTableName = s"${databasesConfig.getString("landing")}.$tableName"
    val partitions = Seq("pt_country", "pt_year", "pt_month", "pt_day", "pt_secs")
    val tableExists = spark.catalog.tableExists(landingTableName)
    val schema = SparkUtils.getSchema(spark, params, landingTableName, tableExists, interfaceName, config)

    val landedDataS3 = SparkUtils.returnFileAsDataframe(spark, objectS3, schema)
      .withColumn("pt_country", lit(params.country()))
      .withColumn("pt_year", lit((jsonAttributes \ "year").as[Int]))
      .withColumn("pt_month", lit((jsonAttributes \ "month").as[Int]))
      .withColumn("pt_day", lit((jsonAttributes \ "day").as[Int]))
      .withColumn("pt_secs", lit((jsonAttributes \ "secs").as[Int]))

    landedDataS3
  }
}