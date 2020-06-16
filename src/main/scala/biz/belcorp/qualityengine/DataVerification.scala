package biz.belcorp.qualityengine

import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.{ConstrainableDataTypes, Constraint}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.types.{StructField, StructType, _}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{DataFrame, SparkSession}

private[qualityengine] object DataVerification {

  def run(session: SparkSession, interfaceDataFrame: DataFrame, rulesDataFile:  String) = {

    // The belcorp data quality rules on which we will compute metrics

    val fieldSchemaType = StructType(
      Array(
        StructField("source", StringType, true),
        StructField("field", StringType, true),
        StructField("isComplete", StringType, true),
        StructField("hasCompleteness", StringType, true),
        StructField("isUnique", StringType, true),
        StructField("isPrimaryKey", StringType, true),
        StructField("hasUniqueness", StringType, true),
        StructField("hasDistinctness", StringType, true),
        StructField("hasUniqueValueRatio", StringType, true),
        StructField("hasNumberOfDistinctValues", StringType, true),
        StructField("hasHistogramValues", StringType, true),
        StructField("hasEntropy", StringType, true),
        StructField("hasApproxQuantile", StringType, true),
        StructField("hasMinLength", StringType, true),
        StructField("hasMaxLength", StringType, true),
        StructField("hasMin", StringType, true),
        StructField("hasMax", StringType, true),
        StructField("hasMean", StringType, true),
        StructField("hasSum", StringType, true),
        StructField("hasStandardDeviation", StringType, true),
        StructField("hasApproxCountDistinct", StringType, true),
        StructField("hasCorrelation", StringType, true),
        StructField("satisfies", StringType, true),
        StructField("hasPattern", StringType, true),
        StructField("containsCreditCardNumber", StringType, true),
        StructField("containsEmail", StringType, true),
        StructField("containsURL", StringType, true),
        StructField("containsSocialSecurityNumber", StringType, true),
        StructField("hasDataType", StringType, true),
        StructField("isNonNegative", StringType, true),
        StructField("isPositive", StringType, true),
        StructField("isLessThan", StringType, true),
        StructField("isLessThanOrEqualTo", StringType, true),
        StructField("isGreaterThanOrEqualTo", StringType, true),
        StructField("isContainedIn", StringType, true)
      )
    )

    val rulesSchema = StructType(fieldSchemaType)
    val rulesDataFrame = session.read.format("csv")
      .option("delimiter",";")
      .option("header", "true")
      .schema(rulesSchema)
      .load(rulesDataFile)

    val verificationResult: VerificationResult = {
    val _checks = new ListBuffer[Check]()
    rulesDataFrame.show(truncate=false)
    rulesDataFrame.collect().foreach { row =>

      val fieldName = row.getString(1)
       row.toSeq.toArray.zipWithIndex.foreach{ case(item,index) =>
         if(null!=item){
            val rule = index match {
              case 2  => if(item.equals("s")) _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").isComplete(fieldName)
              case 3  => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").hasCompleteness(fieldName, _ >= item.toString.toDouble)
              case 4  => if(item.equals("s")) _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").isUnique(fieldName)
              case 5  => if(item.equals("s")) _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").isPrimaryKey(fieldName)
              case 9  => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").hasNumberOfDistinctValues(fieldName, _ >= item.toString.toDouble)
              case 11 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").hasEntropy(fieldName, _ >= item.toString.toDouble)
              case 12 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").hasApproxQuantile(fieldName, 0.5, _ >= item.toString.toDouble)
              case 13 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").hasMinLength(fieldName, _== item.toString.toDouble)
              case 14 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").hasMaxLength(fieldName, _== item.toString.toDouble)
              case 15 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").hasMin(fieldName, _ == item.toString.toDouble)
              case 16 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").hasMax(fieldName, _ == item.toString.toDouble)
              case 17 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").hasMean(fieldName, _== item.toString.toDouble)
              case 18 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").hasSum(fieldName, _== item.toString.toDouble)
              case 19 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").hasStandardDeviation(fieldName, _<= item.toString.toDouble)
              case 20 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").hasApproxCountDistinct(fieldName, _<= item.toString.toDouble)
              case 23 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").hasPattern(fieldName, item.toString.r)
              case 24 => if(item.equals("s"))_checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").containsCreditCardNumber(fieldName)
              case 25 => if(item.equals("s"))_checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").containsEmail(fieldName)
              case 26 => if(item.equals("s"))_checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").containsURL(fieldName)
              case 27 => if(item.equals("s"))_checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").containsSocialSecurityNumber(fieldName)
              case 28 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").hasDataType(fieldName, ConstrainableDataTypes.Integral)
              case 29 => if(item.equals("s"))_checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").isNonNegative(fieldName)
              case 30 => if(item.equals("s"))_checks+=  Check(CheckLevel.Error, s"$index: $fieldName checks").isPositive(fieldName)
              case 31 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").isLessThan(fieldName, "")
              case 32 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").isLessThanOrEqualTo(fieldName, "")
              case 33 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").isGreaterThan(fieldName, "")
              case 34 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").isGreaterThanOrEqualTo(fieldName, "")
              case 35 => _checks+=  Check(CheckLevel.Error, s"$index: $fieldName  checks").isContainedIn(fieldName,Array[String]("x","t"))
              case _  => "Invalid rule"  // the default, catch-all
            }
            //println(rule)
          }
        }
      }

    VerificationSuite()
      .onData(interfaceDataFrame)
      .addChecks( _checks)
      .run()
    }
    val resultDataFrame = checkResultsAsDataFrame(session, verificationResult)
    resultDataFrame.show(truncate=false)
    resultDataFrame
  }
}