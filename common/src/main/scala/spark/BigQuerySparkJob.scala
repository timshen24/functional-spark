package spark

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.codec.binary.Base64
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.nio.charset.StandardCharsets
import java.util

import common.GeneralColumnConstants._

trait SparkJob {
  lazy val spark: SparkSession = getSparkSession

  private def getSparkSession: SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName("analytics-common-spark")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.driver.host", "127.0.0.1")
//      .set("spark.sql.sources.ignoreDataLocality", "true")
      .registerKryoClasses(Array(classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult], classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary], classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats], Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"), classOf[Array[org.apache.spark.sql.catalyst.InternalRow]], classOf[InternalRow], classOf[Array[Array[Row]]], classOf[Array[Row]], classOf[Row], classOf[GenericRowWithSchema], classOf[StructType], classOf[Array[StructType]], classOf[Array[StructField]], classOf[StructField], classOf[IntegerType], classOf[Metadata], Class.forName("org.apache.spark.sql.types.StringType$"), Class.forName("org.apache.spark.sql.types.IntegerType$"), Class.forName("org.apache.spark.sql.types.LongType$"), Class.forName("org.apache.spark.sql.types.BooleanType$"), Class.forName("org.apache.spark.sql.types.DoubleType$"), Class.forName("org.apache.spark.sql.types.ArrayType"), Class.forName("org.apache.spark.sql.types.TimestampType$"), classOf[LongHashedRelation], Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"), Class.forName("org.apache.spark.sql.execution.joins.EmptyHashedRelation$"), Class.forName("org.apache.spark.sql.delta.actions.AddFile"), Class.forName("scala.collection.mutable.ListBuffer"), Class.forName("org.apache.spark.sql.execution.columnar.DefaultCachedBatch"), Class.forName("[[B"), Class.forName("org.apache.spark.sql.catalyst.expressions.GenericInternalRow"), Class.forName("org.apache.spark.unsafe.types.UTF8String"), Class.forName("scala.math.Ordering$Reverse"), Class.forName("org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering"), classOf[Array[org.apache.spark.sql.catalyst.expressions.SortOrder]], Class.forName("org.apache.spark.sql.catalyst.expressions.SortOrder"), Class.forName("org.apache.spark.sql.catalyst.expressions.BoundReference"), Class.forName("java.lang.invoke.SerializedLambda"), Class.forName("org.apache.spark.sql.catalyst.InternalRow$"), Class.forName("org.apache.spark.sql.catalyst.trees.Origin"), Class.forName("org.apache.spark.sql.catalyst.expressions.Ascending$"), Class.forName("org.apache.spark.sql.catalyst.expressions.NullsFirst$")))
    val spark: SparkSession = if (sys.env.getOrElse("dq_env", "prod") == "dev") {
      SparkSession.builder()
        .master("local[*]")
        .config(sparkConf)
        .getOrCreate()
    } else {
      SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
    }
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}

trait BigQuerySparkJob extends SparkJob {
  val credentials: String
  lazy val credentialsMap: util.Map[String, String] = json2Map(credentials)

  protected def initParams(): Unit = {
    spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.enable", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.gs.project.id", credentialsMap.get("project_id"))
    spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.email", credentialsMap.get("client_email"))
    spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.private.key.id", credentialsMap.get("private_key_id"))
    spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.private.key", credentialsMap.get("private_key"))
    spark.sparkContext.hadoopConfiguration.set("parentProject", credentialsMap.get("project_id"))
    spark.sparkContext.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationProject", credentialsMap.get("project_id"))
    spark.conf.set("materializationDataset", "temp")
  }

  private def json2Map(json: String): util.Map[String, String] = {
    val mapper: ObjectMapper = new ObjectMapper
    var map: util.Map[String, String] = new util.HashMap[String, String](32)
    try map = mapper.readValue(json, classOf[util.Map[String, String]])
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    map
  }

  implicit class DataFrameExtension(df: DataFrame) {
    def writeToBQ(dest: String, mode: String = "overwrite"): Unit = {
      df.write
        .format("bigquery")
        .option("partitionType", "DAY")
        .option("partitionField", DATE_PARTITION)
        .option("temporaryGcsBucket", "prd-data-analytics-.../temp")
        .option("intermediateFormat", "orc")
        .option("parentProject", credentialsMap.get("project_id"))
        .option("credentials", new String(Base64.encodeBase64(credentials.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8))
        .option("createDisposition", "CREATE_IF_NEEDED")
        .mode(mode)
        .save(dest)
    }
  }
}


