import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, sum}

import java.sql.Date

object FunctionalSpark extends App {
  val spark = SparkSession.builder()
  .master("local[*]")
  .config(new SparkConf())
  .getOrCreate()

  import spark.implicits._

  // shortcut for DF to DS
  def toDS[T <: Product : Encoder](df: DataFrame): Dataset[T] = df.as[T]

  final case class Person(
                           personId: Int,
                           firstName: String,
                           lastName: String)

  final case class Sales(
                          date: Date,
                          personId: Int,
                          customerName: String,
                          amountDollars: Double)

  val personData: Seq[Row] = Seq(
    Row(1, "Eric", "Tome"),
    Row(2, "Jennifer", "C"),
    Row(3, "Cara", "Rae")
  )
  val salesData: Seq[Row] = Seq(
    Row(new Date(1577858400000L), 1, "Third Bank", 100.29),
    Row(new Date(1585717200000L), 3, "Pet's Paradise", 1233451.33),
    Row(new Date(1585717200000L), 2, "Small Shoes", 4543.35),
    Row(new Date(1593579600000L), 1, "PaperCo", 84990.15),
    Row(new Date(1601528400000L), 1, "Disco Balls'r'us", 504.00),
    Row(new Date(1601528400000L), 2, "Big Shovels", 9.99)
  )

  private val personSchema: StructType = Encoders.product[Person].schema
  private val salesSchema: StructType  = Encoders.product[Sales].schema

  def getDSFromSeq[T <: Product: Encoder](data: Seq[Row], schema: StructType) =
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      ).as[T]

  val personDS: Dataset[Person] = getDSFromSeq[Person](personData, personSchema)
  val salesDS: Dataset[Sales] = getDSFromSeq[Sales](salesData, salesSchema)

  personDS.filter(r => r.firstName.contains("Eric"))

  salesDS.filter(r => r.personId.equals(1))

  salesDS.filter(r => r.amountDollars > 100)

  salesDS.filter(r => r.amountDollars > 600 && r.amountDollars < 5000)

  personDS.show
  salesDS.show

  final case class SalesChangeColumnNames(
                                           SALES_DATE: Date,
                                           PERSON_ID: Int,
                                           CUSTOMER_NAME: String,
                                           SALES_IN_DOLLARS: Double)
  def saleColumns: Map[String, String] =
    Map(
      "date"          -> "SALES_DATE",
      "personId"      -> "PERSON_ID",
      "customerName"  -> "CUSTOMER_NAME",
      "amountDollars" -> "SALES_IN_DOLLARS"
    )
  def renameColumns(ds: Dataset[Sales], m: Map[String, String]): Dataset[SalesChangeColumnNames] =
    toDS {
      m.foldLeft(ds.toDF())((acc, colnames) => acc.withColumnRenamed(colnames._1, colnames._2))
    }
  renameColumns(salesDS, saleColumns).show

  final case class JoinedData(
                               personId: Int,
                               firstName: String,
                               lastName: String,
                               date: Date,
                               customerName: String,
                               amountDollars: Double)
  val joinedData: Dataset[JoinedData] =
    toDS(personDS.join(salesDS, Seq("personId"), "left"))


  final case class JoinedDataWithEuro(
                                       date: Date,
                                       personId: Int,
                                       firstName: String,
                                       lastName: String,
                                       initials: String,
                                       customerName: String,
                                       amountDollars: Double,
                                       amountEuros: Double)
  def dollarToEuro(d: Double): Double = d * 1.19
  def initials(firstName: String, lastName: String): String =
    s"${firstName.substring(0, 1)}${lastName.substring(0, 1)}"
  val joinedDataWithEuro: Dataset[JoinedDataWithEuro] =
    joinedData.map(r =>
      JoinedDataWithEuro(
        r.date,
        r.personId,
        r.firstName.toUpperCase(), // modified column
        r.lastName.toLowerCase(), // modified column
        initials(r.firstName, r.lastName), // new column
        r.customerName.trim(), // modified column
        r.amountDollars,
        dollarToEuro(r.amountDollars) // new column
      )
    )

  final case class TotalSalesByPerson(
                                       personId: Int,
                                       firstName: String,
                                       lastName: String,
                                       initials: String,
                                       sumAmountDollars: Double,
                                       sumAmountEuros: Double,
                                       avgAmountDollars: Double,
                                       avgAmountEuros: Double)
  val totalSalesByPerson: Dataset[TotalSalesByPerson] =
    toDS {
      joinedDataWithEuro
        .groupBy($"personId", $"firstName", $"lastName", $"initials").agg(
        sum($"amountDollars").alias("sumAmountDollars"),
        sum($"amountEuros").alias("sumAmountEuros"),
        avg($"amountDollars").alias("avgAmountDollars"),
        avg($"amountEuros").alias("avgAmountEuros")
      )
    }

  totalSalesByPerson.show
}
