package biz.belcorp.qualityengine.util

import org.apache.spark.sql.SparkSession

private[qualityengine] object ExecutionUtils {
    def withSpark(func: SparkSession => Unit): Unit = {
      val session = SparkSession.builder()
        .master("local")
        .appName("DemoDeequ")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
      session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))
      try {
        func(session)
      } finally {
        session.stop()
        System.clearProperty("spark.driver.port")
      }
    }

  def withYarn(func: SparkSession => Unit): Unit = {
    val session = SparkSession.builder()
      .master("yarn")
      .appName("DemoDeequ")
      .config("deploy-mode", "master")
      .getOrCreate()
    try {
      func(session)
    } finally {
      session.stop()
    }
  }

}