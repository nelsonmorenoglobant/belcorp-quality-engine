package biz.belcorp.qualityengine

import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.{ConstrainableDataTypes, Constraint}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.types.{StructField, StructType, _}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.matching.Regex

private[qualityengine] object DataVerification {

  /**
   * Perform the data quality constraints verifications
   *
   * @author Nelson Moreno
   * @param session the spark session
   * @param interfaceDataFrame the spark dataframe containing the inteface data.
   * @param rulesDataFile A csv file constraints of the interface
   * @return
   */

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
        StructField("hasMin", LongType, true),
        StructField("hasMax", LongType, true),
        StructField("hasMean", LongType, true),
        StructField("hasSum", LongType, true),
        StructField("hasStandardDeviation", LongType, true),
        StructField("hasApproxCountDistinct", StringType, true),
        StructField("hasCorrelation", StringType, true),
        StructField("satisfies", StringType, true),
        StructField("hasPattern", StringType, true),
        StructField("containsCreditCardNumber", StringType, true),
        StructField("containsEmail", StringType, true),
        StructField("containsURL", StringType, true),
        StructField("containsSocialSecurityNumber", StringType, true),
        StructField("hasDataType", StringType, true),
        StructField("isNonNegative", LongType, true),
        StructField("isPositive", LongType, true),
        StructField("isLessThan", LongType, true),
        StructField("isLessThanOrEqualTo", LongType, true),
        StructField("isGreaterThanOrEqualTo", LongType, true),
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
       row.toSeq.toArray.zipWithIndex.foreach{ case(fieldValue,index) =>
         if(null!=fieldValue){
            val rule = index match {
              case 2  => if(fieldValue.equals("s")) _checks+= isComplete(fieldName, index)
              case 3  => _checks+=  hasCompleteness(fieldName, fieldValue.toString.toDouble, index)
              case 4  => if(fieldValue.equals("s")) _checks+=  isUnique(fieldName, index)
              case 5  => if(fieldValue.equals("s")) _checks+=  isPrimaryKey(fieldName, index)
              case 9  => _checks+=  hasNumberOfDistinctValues(fieldName, fieldValue.toString.toLong, index)
              case 11 => _checks+=  hasEntropy(fieldName, fieldValue.toString.toDouble, index)
              case 12 => _checks+=  hasApproxQuantile(fieldName, 0.5, fieldValue.toString.toDouble, index)
              case 13 => _checks+=  hasMinLength(fieldName, fieldValue.toString.toDouble, index)
              case 14 => _checks+=  hasMaxLength(fieldName, fieldValue.toString.toDouble, index)
              case 15 => _checks+=  hasMin(fieldName, fieldValue.toString.toDouble, index)
              case 16 => _checks+=  hasMax(fieldName, fieldValue.toString.toDouble, index)
              case 17 => _checks+=  hasMean(fieldName, fieldValue.toString.toDouble, index)
              case 18 => _checks+=  hasSum(fieldName, fieldValue.toString.toDouble, index)
              case 19 => _checks+=  hasStandardDeviation(fieldName, fieldValue.toString.toDouble, index)
              case 20 => _checks+=  hasApproxCountDistinct(fieldName, fieldValue.toString.toDouble, index)
              case 23 => _checks+=  hasPattern(fieldName, fieldValue.toString.r, index)
              case 24 => if(fieldValue.equals("s"))_checks+=  containsCreditCardNumber(fieldName, index)
              case 25 => if(fieldValue.equals("s"))_checks+=  containsEmail(fieldName, index)
              case 26 => if(fieldValue.equals("s"))_checks+=  containsURL(fieldName, index)
              case 27 => if(fieldValue.equals("s"))_checks+=  containsSocialSecurityNumber(fieldName, index)
              case 28 => _checks+= hasDataType(fieldName, ConstrainableDataTypes.Integral, index)
              case 29 => if(fieldValue.equals("s"))_checks+= isNonNegative(fieldName, index)
              case 30 => if(fieldValue.equals("s"))_checks+= isPositive(fieldName, index)
              case 31 => _checks+=  isLessThan(fieldName, fieldName, index)
              case 32 => _checks+=  isLessThanOrEqualTo(fieldName, fieldName, index)
              case 33 => _checks+=  isGreaterThan(fieldName, fieldName, index)
              case 34 => _checks+=  isGreaterThanOrEqualTo(fieldName, fieldName, index)
              case 35 => _checks+=  isContainedIn(fieldName,fieldValue.toString.split(",").map(_.trim), index)
              case _  => "Invalid rule"  // the default, catch-all
            }
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

  /**
   * Creates a constraint that asserts on a field completion.
   *
   * @param fieldName to run the assertion on
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def isComplete (fieldName: String, index: Int, hint: Option[String] = None): Check ={
    Check(CheckLevel.Error, s"$index: $fieldName checks").isComplete(fieldName, hint)
  }

  /**
   * Creates a constraint that asserts on a column completion.
   * Uses the given history selection strategy to retrieve historical completeness values on this
   * column from the history provider.
   *
   * @param fieldName to run the assertion on
   * @param fieldValue a double input [0 to 1] parameter to compare with the completeness results
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasCompleteness(
                       fieldName: String,
                       fieldValue: Double,
                       index: Int,
                       hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasCompleteness(fieldName, _ >= fieldValue.toString.toDouble, hint)
  }

  /**
   * Creates a constraint that asserts on a column uniqueness.
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def isUnique(
                fieldName: String,
                index: Int,
                hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").isUnique(fieldName, hint)
  }

  /**
   * Creates a constraint that asserts on a column(s) primary key characteristics.
   * Currently only checks uniqueness, but reserved for primary key checks if there is another
   * assertion to run on primary key columns.
   *
   * @author Nelson Moreno
   * @param fieldName Columns to run the assertion on
   * @param index The rule excel column position
   * @return
   */
  def isPrimaryKey(fieldName: String, index: Int): Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").isPrimaryKey(fieldName)
  }

  /**
   * Creates a constraint that asserts on the number of distinct values a column has.
   *
   * @author Nelson Moreno
   * @param fieldName     Column to run the assertion on
   * @param fieldValue a Long value to compare with the Number Of Distinct Values results
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasNumberOfDistinctValues(
                                 fieldName: String,
                                 fieldValue: Long,
                                 index: Int,
                                 hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasNumberOfDistinctValues(fieldName, _ >= fieldValue.toString.toDouble)
  }

  /**
   * Creates a constraint that asserts on a column entropy.
   *
   * @author Nelson Moreno
   * @param fieldName    Column to run the assertion on
   * @param fieldValue a double input [0 to 1] parameter to compare with the Entropy results
   * @param index The rule excel column position
   * @param hint      A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasEntropy(
                  fieldName: String,
                  fieldValue: Double,
                  index: Int,
                  hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasEntropy(fieldName, _ >= fieldValue, hint)
  }

  /**
   * Creates a constraint that asserts on an approximated quantile
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param quantile Which quantile to assert on
   * @param fieldValue a double input [0 to 1] parameter to compare with the Approx Quantile results
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasApproxQuantile(fieldName: String,
                        quantile: Double,
                        fieldValue: Double,
                        index: Int,
                        hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasApproxQuantile(fieldName, 0.5, _ >= fieldValue, hint)
  }

  /**
   * Creates a constraint that asserts on the minimum length of the column
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param fieldValue a Double parameter to compare with the Min Length results
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasMinLength(
                    fieldName: String,
                    fieldValue: Double,
                    index: Int,
                    hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasMinLength(fieldName, _== fieldValue, hint)
  }

  /**
   * Creates a constraint that asserts on the maximum length of the column
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param fieldValue a Double parameter to compare with the Max Length results
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasMaxLength(
                    fieldName: String,
                    fieldValue: Double,
                    index: Int,
                    hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasMaxLength(fieldName, _== fieldValue.toString.toDouble, hint)
  }

  /**
   * Creates a constraint that asserts on the minimum of the column
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param fieldValue a Double parameter to compare with the Min results
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasMin(
              fieldName: String,
              fieldValue: Double,
              index: Int,
              hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasMin(fieldName, _ == fieldValue.toString.toDouble, hint)
  }

  /**
   * Creates a constraint that asserts on the maximum of the column
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param fieldValue a Double parameter to compare with the Max results
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasMax(
              fieldName: String,
              fieldValue: Double,
              index: Int,
              hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasMax(fieldName, _ == fieldValue.toString.toDouble, hint)
  }

  /**
   * Creates a constraint that asserts on the mean of the column
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param fieldValue a Double parameter to compare with the Mean results
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasMean(
               fieldName: String,
               fieldValue: Double,
               index: Int,
               hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasMean(fieldName, _== fieldValue.toString.toDouble, hint)
  }

  /**
   * Creates a constraint that asserts on the sum of the column
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param fieldValue a Double parameter to compare with the Sum results
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasSum(
              fieldName: String,
              fieldValue: Double,
              index: Int,
              hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasSum(fieldName, _== fieldValue.toString.toDouble, hint)
  }

  /**
   * Creates a constraint that asserts on the standard deviation of the column
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param fieldValue a Double parameter to compare with the Standard Deviation results
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasStandardDeviation(
                            fieldName: String,
                            fieldValue: Double,
                            index: Int,
                            hint: Option[String] = None
                          )
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasStandardDeviation(fieldName, _<= fieldValue.toString.toDouble, hint)
  }

  /**
   * Creates a constraint that asserts on the approximate count distinct of the given column
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param fieldValue a Double parameter to compare with the Approx Count Distinct results
   * @param index The rule excel column position
   * @param hint A hint to provide additional context why a constraint could have failed
   * @return
   */
  def hasApproxCountDistinct(
                              fieldName: String,
                              fieldValue: Double,
                              index: Int,
                              hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasApproxCountDistinct(fieldName, _<= fieldValue.toString.toDouble, hint)
  }

  /**
   * Checks for pattern compliance. Given a column name and a regular expression, defines a
   * Check on the average compliance of the column's values to the regular expression.
   *
   * @author Nelson Moreno
   * @param fieldName Name of the column that should be checked.
   * @param fieldValue The columns values will be checked for a match against this pattern.
   * @param index The rule excel column position
   * @return
   */
  def hasPattern(
                  fieldName: String,
                  fieldValue: Regex,
                  index: Int)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").hasPattern(fieldName, fieldValue.toString.r)
  }

  /**
   * Check to run against the compliance of a column against a Credit Card pattern.
   *
   * @author Nelson Moreno
   * @param fieldName Name of the column that should be checked.
   * @param index The rule excel column position
   * @return
   */
  def containsCreditCardNumber(
                                fieldName: String,
                                index: Int)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName  checks").containsCreditCardNumber(fieldName)
  }

  /**
   * Check to run against the compliance of a column against an e-mail pattern.
   *
   * @author Nelson Moreno
   * @param fieldName Name of the column that should be checked.
   * @param index The rule excel column position
   * @return
   */
  def containsEmail(
                     fieldName: String,
                     index: Int)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName  checks").containsEmail(fieldName)
  }

  /**
   * Check to run against the compliance of a column against an URL pattern.
   *
   * @author Nelson Moreno
   * @param fieldName Name of the column that should be checked.
   * @param index The rule excel column position
   * @return
   */
  def containsURL(
                   fieldName: String,
                   index: Int)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName checks").containsURL(fieldName)
  }

  /**
   * Check to run against the compliance of a column against the Social security number pattern
   * for the US.
   *
   * @author Nelson Moreno
   * @param fieldName Name of the column that should be checked.
   * @param index The rule excel column position
   * @return
   */
  def containsSocialSecurityNumber(
                                    fieldName: String,
                                    index: Int)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName  checks").containsSocialSecurityNumber(fieldName)
  }

  /**
   * Check to run against the fraction of rows that conform to the given data type.
   *
   * @author Nelson Moreno
   * @param fieldName Name of the columns that should be checked.
   * @param dataType Data type that the columns should be compared against.
   * @param index The rule excel column position
   * @return
   */
  def hasDataType(
                   fieldName: String,
                   dataType: ConstrainableDataTypes.Value,
                   index: Int)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName  checks").hasDataType(fieldName, dataType)
  }


  /**
   * Creates a constraint that asserts that a column contains no negative values
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param index The rule excel column position
   * @return
   */
  def isNonNegative(
                     fieldName: String,
                     index: Int)
  : Check = {

    // coalescing column to not count NULL values as non-compliant
    Check(CheckLevel.Error, s"$index: $fieldName  checks").isNonNegative(fieldName)
  }

  /**
   * Creates a constraint that asserts that a column contains no negative values
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param index The rule excel column position
   * @return
   */
  def isPositive(
                  fieldName: String,
                  index: Int)
  : Check = {
    // coalescing column to not count NULL values as non-compliant
    Check(CheckLevel.Error, s"$index: $fieldName  checks").isPositive(fieldName)
  }


  /**
   *
   * Asserts that, in each row, the value of columnA is less than the value of columnB
   *
   * @author Nelson Moreno
   * @param fieldNameA Column to run the assertion on
   * @param fieldNameB Column to run the assertion on
   * @param index The rule excel column position
   * @return
   */
  def isLessThan(
                  fieldNameA: String,
                  fieldNameB: String,
                  index: Int)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldNameA & $fieldNameB  checks").isLessThan(fieldNameA, fieldNameB)
  }

  /**
   * Asserts that, in each row, the value of columnA is less than or equal to the value of columnB
   *
   * @author Nelson Moreno
   * @param fieldNameA Column to run the assertion on
   * @param fieldNameB Column to run the assertion on
   * @param index The rule excel column position
   * @return
   */
  def isLessThanOrEqualTo(
                           fieldNameA: String,
                           fieldNameB: String,
                           index: Int)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldNameA & $fieldNameB  checks").isLessThan(fieldNameA, fieldNameB)
  }

  /**
   * Asserts that, in each row, the value of columnA is greater than the value of columnB
   *
   * @author Nelson Moreno
   * @param fieldNameA Column to run the assertion on
   * @param fieldNameB Column to run the assertion on
   * @param index The rule excel column position
   * @return
   */
  def isGreaterThan(
                     fieldNameA: String,
                     fieldNameB: String,
                     index: Int)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldNameA & $fieldNameB  checks").isLessThan(fieldNameA, fieldNameB)
  }

  /**
   * Asserts that, in each row, the value of columnA is greather than or equal to the value of
   * columnB
   *
   * @author Nelson Moreno
   * @param fieldNameA Column to run the assertion on
   * @param fieldNameB Column to run the assertion on
   * @param index The rule excel column position
   * @return
   */
  def isGreaterThanOrEqualTo(
                              fieldNameA: String,
                              fieldNameB: String,
                              index: Int)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldNameA & $fieldNameB  checks").isLessThan(fieldNameA, fieldNameB)
  }

  /**
   * Asserts that every non-null value in a column is contained in a set of predefined values
   *
   * @author Nelson Moreno
   * @param fieldName Column to run the assertion on
   * @param allowedValues allowed values for the column
   * @return
   */
  def isContainedIn(
                     fieldName: String,
                     allowedValues: Array[String],
                     index: Int,
                     hint: Option[String] = None)
  : Check = {
    Check(CheckLevel.Error, s"$index: $fieldName  checks").isContainedIn(fieldName, allowedValues, hint)
  }
}