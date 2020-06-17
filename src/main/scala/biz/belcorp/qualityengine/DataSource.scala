package biz.belcorp.qualityengine

import biz.belcorp.dl.utils.{Params, SparkUtils}
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.JsValue


class DataSource(spark: SparkSession) {

  /**
   * build a spark interface dataframe.
   *
   * @author Nelson Moreno
   * @param interfaceName the spark dataframe containing the fmt data.
   * @param objectS3 s3 path of the data.
   * @param params the spark program execution parameters
   * @param config set of enviroment params
   * @return
   */

  def getDataFrame(interfaceName: String, objectS3: String, params: Params, config: Config): DataFrame = {
    val databasesConfig = config.getConfig("databases")
    val tableName = s"${params.system()}_$interfaceName"
    val landingTableName = s"${databasesConfig.getString("landing")}.$tableName"
    val partitions = Seq("pt_country", "pt_year", "pt_month", "pt_day", "pt_secs")
    val tableExists = spark.catalog.tableExists(landingTableName)
    val schema = SparkUtils.getSchema(spark, params, landingTableName, tableExists, interfaceName, config)
    val landedDataS3 = SparkUtils.returnFileAsDataframe(spark, objectS3, schema)
    landedDataS3
  }

}