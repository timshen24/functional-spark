package data.etl

import data.io.IO
import data.monad.Reader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp}

trait ETL extends IO {
  case class UserJourneyConfig(startDate: String, prevPath: String, destPath: String, prevStep: Seq[String] => DataFrame, trans: DataFrame => DataFrame)

  def getSourceDf(configRepo: UserJourneyConfig): DataFrame =
    configRepo.prevStep(Seq(conf.getString(configRepo.prevPath)))
      .where(col("datepartition") >= configRepo.startDate)

  def etl: Reader[UserJourneyConfig, Unit] = Reader { configRepo =>
    require(configRepo.startDate.nonEmpty, "startDate must not be empty!")
    getSourceDf(configRepo)
      .transform(configRepo.trans)
      .select(col("*"), current_timestamp().as("update_time"))
      .saveAndOverwritePartitions(configRepo.startDate)(configRepo.destPath)
  }
}
