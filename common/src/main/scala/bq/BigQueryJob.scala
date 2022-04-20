package bq

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object BigQueryJob {
  def deletePartitions(start: LocalDate, end: LocalDate = LocalDate.now())(bigquery: BigQuery)(dataset: String)(table: String): Unit = {
    val dates = for (i <- 0L to ChronoUnit.DAYS.between(start, end))
      yield start.plusDays(i).format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    println(dates)

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent._
    import scala.util._
    def deleteOnePartiton(date: String): Future[Boolean] = {
      Future(bigquery.delete(TableId.of(dataset, s"$table$$$date")))
    }

    Future.traverse(dates.toList)(deleteOnePartiton).onComplete {
      case Failure(t) => throw t
      case Success(v) => println(s"The result of deleted BigQuery partitions are $v")
    }
  }
}
