package data.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.matching.Regex

object HelperFunctions {
  def nextDiffValueInLead: DataFrame => DataFrame = {
//    import spark.implicits._
    df => df.withColumn("temp_rank", monotonically_increasing_id)
      .withColumn("next_page_view_attribution_flag",
        lead("attribution_flag", 1, 0)
          .over(Window.partitionBy("session_final_id").orderBy("event_time", "depth", "temp_rank"))
      )
      .withColumn("next_page_view_attribution_flag",
        last("next_page_view_attribution_flag")
          .over(Window.partitionBy("session_final_id", "unique_view_id")
            .orderBy("event_time", "depth", "temp_rank")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
      )
      .drop("temp_rank")
  }

  val typeRegex: Regex = "listing.*navigation".r
  val subTypeRegex: Regex = "listing.*refine".r

  def isBack(exitInteraction: Option[String]): Boolean =
    exitInteraction match {
      case Some(null) => false
      case Some(value) => value.toLowerCase == "back"
    }

  def isBackground(exitInteraction: Option[String]): Boolean =
    exitInteraction match {
      case Some(null) => false
      case Some(value) => value.toLowerCase == "background"
    }

  def isPLP(oType: String): Boolean = Set("Curated Listing", "Listing", "Search Listing").contains(oType)

  def isFilter(oType: String, subType: String): Boolean = {
    typeRegex.pattern.matcher(oType.toLowerCase()).matches() && subTypeRegex.pattern.matcher(subType.toLowerCase()).matches()
  }
}
