package data.etl.impl1

import data.io.IODev
import data.monad.Reader
import data.renderedPrint
import org.apache.spark.sql.DataFrame

trait Impl1ETLDev extends Impl1ETL {
  self: IODev =>

  override def getSometracking(omniPaths: String*): DataFrame =
    spark.read.format("csv").option("header", "true").load(omniPaths: _*)

  override def etl: Reader[CustomRepoConfig, Unit] = Reader { _ =>
    val omniBasePaths = Seq(conf.getString("baseDF.dir"))
    renderedPrint(s"omniBasePath = $omniBasePaths")
    step0(omniBasePaths)
      .transform(transformTable1)
      .show(200, truncate = false)
  }
}
