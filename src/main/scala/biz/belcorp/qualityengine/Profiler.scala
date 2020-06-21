package biz.belcorp.qualityengine

import biz.belcorp.dl.logs.{EsLogger, logEvent}
import biz.belcorp.dl.logs.validateConnection._
import biz.belcorp.dl.utils.{EnvUtils, Params, SparkUtils}
import biz.belcorp.dl.utils.Extensions._
import biz.belcorp.dl.utils.ConfigUtils
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import play.api.libs.json.{JsValue, Json}


object Profiler {

  def main(args: Array[String]): Unit = {

    val params = new Params(args)
    val config = ConfigUtils.getConfig(params.environment())
    val spark = SparkUtils.getSparkSession(config)
    val ingestAttributes: JsValue = Json.parse(params.ingest_attributes())
    val logEvent = new logEvent(params)
    val payloadRow: String = logEvent.payload(ingestAttributes.toString())
    val interfaces = ingestAttributes("Interfaces").as[Seq[String]]
    val appLogger = Logger.getLogger("datalake")

    appLogger.info("Initiation profiling Process")

    val dataSource = new DataSource(spark)
    val s3InputPath = config.getConfig("pathS3").getString("inputPath")
    val unzippedFilesPath = s"$s3InputPath$params.system()/$params.country()/unzipped/$params.id_carga()/$params.uuidFile()"
    val appID = spark.sparkContext.applicationId

    val logger = new EsLogger(params, config)
    logger.success("PROFILING_DATA",appID).done()

    try {
      val fileSystem = FileSystem.get(URI.create(unzippedFilesPath), new Configuration())
      val unzippedFiles = fileSystem.listStatus(new Path(unzippedFilesPath)).map(_.getPath.toString)
      val filesToProcess = unzippedFiles.withFilter(uz => interfaces.contains(uz.getInterface)).map(a => (a.getInterface, a))

      logger.success(s"profiling ${filesToProcess.length} files",appID).done()

      val qualityRulesDataframe = dataSource.getQualityRulesAsDataFrame(config.getString("quality-engine.url_rules"))

      filesToProcess.foreach {
        case (intf, infPath) =>
          val interfasDataframe = dataSource.getInterfaceDataFrame(intf, infPath, params, config)

          val analysisProfiler =  DataAnalysis
          val profilerResults = analysisProfiler.run(spark, interfasDataframe)
          val profileResultsPath = config.getString("quality-engine.url_profile_results")

          saveDataFrameResultsAsSingleCsv(profilerResults, s"$profileResultsPath$intf.profile.csv")

          val dataVerification =  DataVerification
          val verificationResults = dataVerification.run(spark, interfasDataframe, qualityRulesDataframe.filter(s"source = '$intf'"))
          val verificationResultsPath = config.getString("quality-engine.url_verification_results")

          saveDataFrameResultsAsSingleCsv(verificationResults, s"$verificationResultsPath$intf.verification.csv")
      }
      logger.success("DATA QUALITY VERIFICATION DONE", appID).done()
      EnvUtils.httpPost(config, "/work", payloadRow)
    } catch {
      case e: Exception =>
        appLogger.error(s"Error, sending log to API log ---- ${e.getMessage}")
        logEvent.errors(e, config, spark)
        logger.failure("LANDING_FAILED", e,appID).done()
    } finally {
      spark.close()
    }
  }

  def saveDataFrameResultsAsSingleCsv(verificationResults: DataFrame, resultsPath: String): Unit ={
    verificationResults.repartition(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header","true").save(resultsPath)
  }
}