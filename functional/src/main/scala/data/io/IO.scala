package data.io

import bq.BigQueryJob.deletePartitions
import common.GeneralColumnConstants._
import spark.BigQuerySparkJob
import com.google.cloud.spark.bigquery.repackaged.com.google.api.services.bigquery.BigqueryScopes
import com.google.cloud.spark.bigquery.repackaged.com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import com.typesafe.config._
import data.config
import data.monad.Reader
import io.delta.tables.DeltaTable
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode}

import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util._

trait IO extends BigQuerySparkJob {
  val conf: Config

  def getTableFromBq(tableName: String): DataFrame = {
    spark.read.format("bigquery")
      .option("credentials", new String(Base64.encodeBase64(credentials.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8))
      .option("parentProject", credentialsMap.get("project_id"))
      .option("viewsEnabled", "true")
      .option("materializationProject", credentialsMap.get("project_id"))
      .option("materializationDataset", "temp")
      .option("materializationExpirationTimeInMinutes", 120)
      .load(tableName)
  }

  implicit class CustomerJourneyDataFrameExtension(df: DataFrame) {
    def saveAndOverwritePartitions(startDate: String)(path: String): Unit = {
      val destPath = conf.getString(path)
      Try(DeltaTable.forPath(spark, destPath).delete(col(DATE_PARTITION) >= startDate)).recover {
        case ex: AnalysisException => ex.printStackTrace()
      }
      df.repartition(col(DATE_PARTITION)).write.partitionBy(DATE_PARTITION).format(FORMAT).mode(SaveMode.Append)
        // .option("replaceWhere", "date >= '2017-01-01' AND date <= '2017-01-31'")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .save(destPath)
    }
  }

  protected case class InputForMergeBQ(bigquery: BigQuery, startDate: String, destPath: String, bqProject: String, bqDataset: String, bqTable: String, endDate: String = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))

  def mergeIntoBQ: Reader[InputForMergeBQ, Unit] = Reader {
    input => {
      val start = LocalDate.parse(input.startDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      deletePartitions(start)(input.bigquery)(input.bqDataset)(input.bqTable)
      spark.read.format(FORMAT).load(input.destPath)
        .where(col("datepartition") >= input.startDate)
        .writeToBQ(s"${input.bqProject}.${input.bqDataset}.${input.bqTable}", mode = "append")
    }
  }
}

trait IODev extends IO {
  override val credentials: String = Source.fromResource(CREDENTIALS_FILE).mkString
  override val conf: Config = config("dev")
}

trait IOProd extends IO {
  override lazy val credentials: String = new String(Base64.decodeBase64(spark.conf.get("spark.gcp.project.data_analytics_cnpct_1.json.key").getBytes))
  initParams()
  override val conf: Config = config("prod")
}

trait BigQueryIOProd extends IOProd {
  val bigquery: BigQuery = BigQueryOptions.newBuilder()
    .setCredentials(ServiceAccountCredentials.fromStream(IOUtils.toInputStream(credentials, "UTF-8")).createScoped(BigqueryScopes.BIGQUERY))
    .setProjectId("prd-data-analytics-cnpct-1")
    .build()
    .getService
}