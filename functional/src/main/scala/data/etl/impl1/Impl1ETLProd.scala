package data.etl.impl1

import data.io.IOProd
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait Impl1ETLProd extends Impl1ETL {
  self: IOProd =>

  initParams()

  override protected def getSometracking(omniPaths: String*): DataFrame = ???

  override def getSourceDf(configRepo: CustomRepoConfig): DataFrame =
    configRepo.prevStep(Seq("some certain paths"))
      .where(col("datepartition") >= configRepo.startDate)
}
