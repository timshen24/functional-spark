package data.etl.impl1

import data.etl.ETL
import org.apache.spark.sql._

import scala.util.Try

trait Impl1ETL extends ETL {
  import spark.implicits._

  lazy val aMappingTable: DataFrame = Try(
    getTableFromBq("some_project.some_dataset.some_tbl1")
  ).getOrElse(
    spark.read.option("header", value = true).csv("some_project/src/main/resources/some_tbl.csv")
  )

  lazy val dimUserBrandId: DataFrame = getTableFromBq("some_project.some_dataset.some_tbl2")

  protected def getSometracking(omniPaths: String*): DataFrame

  def step0(omniBasePath: Seq[String]): DataFrame =
    getSometracking(omniBasePath: _*)

  protected def step1(step0: DataFrame): DataFrame = {
    step0
      .select("*")
  }

  protected def step2(withInsertFake: DataFrame): DataFrame = ???

  protected def step3(step2: DataFrame): DataFrame = ???

  protected def step4(step3: DataFrame): DataFrame = ???

  protected def step5(step4: DataFrame): DataFrame = ???

  protected def step6(step5: DataFrame): DataFrame = ???

  private def insertFake(omnitracking: DataFrame): DataFrame = ???

  private case class VirtualPageConditions(viewAttibutesType: String, viewAttributesSubtype: String, orViewAttributesType: Option[List[String]], trackerIds: List[String], wlOrBag: Boolean)

  private def prepareVirtualTbl: VirtualPageConditions => DataFrame => DataFrame =
    ???

  def transformTable1: DataFrame => DataFrame = step0 => {
    step0
      .transform(step1)
      .unionAll(step0.transform(insertFake))
      .transform(step2 _ andThen step3 andThen step4 andThen step5 andThen step6)
      .distinct
  }
}