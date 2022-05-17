package data.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.matching.Regex

object HelperFunctions {
  def nextDiffValueInLead: DataFrame => DataFrame = {
//    import spark.implicits._
    df => df.withColumn("col1", monotonically_increasing_id)
      .withColumn("col2",
        lead("col3", 1, 0)
          .over(Window.partitionBy("session_final_id").orderBy("col4", "col5", "col6"))
      )
      .withColumn("next_page_view_col3",
        last("next_page_view_col3")
          .over(Window.partitionBy("session_final_id", "unique_view_id")
            .orderBy("col4", "col5", "col6")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
      )
      .drop("col6")
  }

  val typeRegex: Regex = "listing.*navigation".r
  val subTypeRegex: Regex = "listing.*refine".r

  def isFilter(oType: String, subType: String): Boolean = {
    typeRegex.pattern.matcher(oType.toLowerCase()).matches() && subTypeRegex.pattern.matcher(subType.toLowerCase()).matches()
  }

  def isPLP(oType: String): Boolean = Set("str1", "str2", "str3").contains(oType)

  def isBackground(exitInteraction: Option[String]): Boolean =
    exitInteraction match {
      case Some(null) => false
      case Some(value) => value.toLowerCase == "val2"
    }

  def isBack(exitInteraction: Option[String]): Boolean =
    exitInteraction match {
      case Some(null) => false
      case Some(value) => value.toLowerCase == "val1"
    }
}
