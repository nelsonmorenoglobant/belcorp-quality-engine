package biz.belcorp.qualityengine

import com.amazon.deequ.profiles.{ColumnProfilerRunner, NumericColumnProfile}
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{ApproxCountDistinct, ApproxQuantile, ApproxQuantiles, Completeness, DataType, Distinctness, Entropy, Maximum, Mean, Minimum, Size, Sum, UniqueValueRatio, Uniqueness}
import org.apache.spark.sql.{DataFrame, SparkSession}

private[qualityengine] object DataAnalysis  {


  /**
   * Perform the data quality profiling
   *
   * @author Nelson Moreno
   * @param session the spark session
   * @param interfaceDataFrame the spark dataframe containing the inteface data.
   * @return
   */
  def run(session: SparkSession, interfaceDataFrame: DataFrame): DataFrame = {
    /* Make deequ profile this data. It will execute the three passes over the data and avoid
       any shuffles. */
    val result = ColumnProfilerRunner()
      .onData(interfaceDataFrame)
      .run()

    /* We get a profile for each column which allows to inspect the completeness of the column,
       the approximate number of distinct values and the inferred DataTypedatatype. */
    val analysisResult: AnalyzerContext = {
      val analysisRunner = AnalysisRunner
        // data to run the analysis on
        .onData(interfaceDataFrame)
      result.profiles.foreach { case (productName, profile) =>
        val columnName = profile.column

        analysisRunner.addAnalyzer(ApproxCountDistinct(columnName))
        analysisRunner.addAnalyzer(ApproxQuantile(columnName, quantile = 0.5))
        analysisRunner.addAnalyzer(ApproxQuantiles(columnName, quantiles = Seq(0.1, 0.5, 0.9)))
        analysisRunner.addAnalyzer(Completeness(columnName))
        analysisRunner.addAnalyzer(DataType(columnName))
        analysisRunner.addAnalyzer(Distinctness(columnName))
        analysisRunner.addAnalyzer(Entropy(columnName))
        analysisRunner.addAnalyzer(Uniqueness(columnName))
        analysisRunner.addAnalyzer(UniqueValueRatio(columnName))

        if (profile.dataType.toString == "Integral" || profile.dataType.toString == "Fractional") {
          analysisRunner.addAnalyzer(Maximum(columnName))
          analysisRunner.addAnalyzer(Minimum(columnName))
          analysisRunner.addAnalyzer(Mean(columnName))
          analysisRunner.addAnalyzer(Sum(columnName))
        }
      }
      analysisRunner.addAnalyzer(Size())
      analysisRunner.run()
    }

    val metrics = successMetricsAsDataFrame(session, analysisResult)
    metrics.show()
    metrics
  }
}
