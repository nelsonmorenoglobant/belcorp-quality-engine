package biz.belcorp.qualityengine

import biz.belcorp.dl.utils.{Params, SparkUtils}
import com.typesafe.config.Config
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


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

  def getInterfaceDataFrame(interfaceName: String, objectS3: String, params: Params, config: Config): DataFrame = {
    val databasesConfig = config.getConfig("databases")
    val tableName = s"${params.system()}_$interfaceName"
    val landingTableName = s"${databasesConfig.getString("landing")}.$tableName"
    val tableExists = spark.catalog.tableExists(landingTableName)
    val schema = SparkUtils.getSchema(spark, params, landingTableName, tableExists, interfaceName, config)
    val interfaceDataFrame = SparkUtils.returnFileAsDataframe(spark, objectS3, schema)
    interfaceDataFrame
  }

  def getQualityRulesAsDataFrame(rulesUrl: String): DataFrame ={

    // The belcorp data quality rules on which we will compute metrics
    val fieldSchemaType = StructType(
      Array(
        StructField("source", StringType, nullable = false),
        StructField("field", StringType, nullable = false),
        StructField("isComplete", StringType, nullable = true),
        StructField("hasCompleteness", StringType, nullable = true),
        StructField("isUnique", StringType, nullable = true),
        StructField("isPrimaryKey", StringType, nullable = true),
        StructField("hasUniqueness", StringType, nullable = true),
        StructField("hasDistinctness", StringType, nullable = true),
        StructField("hasUniqueValueRatio", StringType, nullable = true),
        StructField("hasNumberOfDistinctValues", StringType, nullable = true),
        StructField("hasHistogramValues", StringType, nullable = true),
        StructField("hasEntropy", StringType, nullable = true),
        StructField("hasApproxQuantile", StringType, nullable = true),
        StructField("hasMinLength", StringType, nullable = true),
        StructField("hasMaxLength", StringType, nullable = true),
        StructField("hasMin", LongType, nullable = true),
        StructField("hasMax", LongType, nullable = true),
        StructField("hasMean", LongType, nullable = true),
        StructField("hasSum", LongType, nullable = true),
        StructField("hasStandardDeviation", LongType, nullable = true),
        StructField("hasApproxCountDistinct", StringType, nullable = true),
        StructField("hasCorrelation", StringType, nullable = true),
        StructField("satisfies", StringType, nullable = true),
        StructField("hasPattern", StringType, nullable = true),
        StructField("containsCreditCardNumber", StringType, nullable = true),
        StructField("containsEmail", StringType, nullable = true),
        StructField("containsURL", StringType, nullable = true),
        StructField("containsSocialSecurityNumber", StringType, nullable = true),
        StructField("hasDataType", StringType, nullable = true),
        StructField("isNonNegative", LongType, nullable = true),
        StructField("isPositive", LongType, nullable = true),
        StructField("isLessThan", LongType, nullable = true),
        StructField("isLessThanOrEqualTo", LongType, nullable = true),
        StructField("isGreaterThanOrEqualTo", LongType, nullable = true),
        StructField("isContainedIn", StringType, nullable = true)
      )
    )
    val rulesSchema = StructType(fieldSchemaType)
    val rulesDataFrame = spark.read.format("csv")
      .option("delimiter",";")
      .option("header", "true")
      .schema(rulesSchema)
      .load(rulesUrl)
    rulesDataFrame
  }

}