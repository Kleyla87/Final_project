import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("FinalProject.com")
      .getOrCreate()
    import spark.implicits._
    //считываем таблицу из csv в DataFrame
    val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
                  .csv("src/main/yellow_tripdata_2020-01.csv")
    //удаляем дубликаты
    val distinctDf = df.distinct()

    //выбираем нужные для расчетов столбцы
    var cleanData = distinctDf.select("tpep_pickup_datetime", "passenger_count", "total_amount")

    //из столбца с датой и временем начала поездки выделяем дату
    cleanData = cleanData.withColumn("date", to_date(col("tpep_pickup_datetime")))

    //выбираем требуемый интервал - с 1 по 31 января 2020 года
    cleanData = cleanData.filter($"date".between("2020-01-01", "2020-01-31"))

    //применим метод abs, чтобы все значения столбца total_amount были неотрицательными
    cleanData = cleanData.withColumn("total_amount", abs(col("total_amount")))

    //удалим строки, в которых не указано количество пасажиров
    cleanData = cleanData.filter("passenger_count is not null")

    //для каждого дня посчитаем общее количество поездок
    val totalCount = cleanData.groupBy("date").count()

    //для каждого дня посчитаем количество поездок без пассажиров и определим
    // самую дешевую и самую дорогую поездку
    val nullCount = cleanData.filter("passenger_count == 0")
                        .groupBy("date")
                        .agg(
                          count("passenger_count").as("percentage_zero"),
                          max("total_amount").as("max_amount_zero"),
                          min("total_amount").as("min_amount_zero")
                        )
    //для каждого дня посчитаем количество поездок с одним пассажиром и определим
    // самую дешевую и самую дорогую поездку
    val oneCount = cleanData.filter("passenger_count == 1")
                      .groupBy("date")
                      .agg(
                        count("passenger_count").as("percentage_1p"),
                        max("total_amount").as("max_amount_1p"),
                        min("total_amount").as("min_amount_1p")
                      )
    //для каждого дня посчитаем количество поездок с двумя пассажирами и определим
    // самую дешевую и самую дорогую поездку
    val twoCount = cleanData.filter("passenger_count == 2")
                       .groupBy("date")
                         .agg(
                           count("passenger_count").as("percentage_2p"),
                           max("total_amount").as("max_amount_2p"),
                           min("total_amount").as("min_amount_2p")
                         )
    //для каждого дня посчитаем количество поездок с тремя пассажирами и определим
    // самую дешевую и самую дорогую поездку
    val threeCount = cleanData.filter("passenger_count == 3")
                         .groupBy("date")
                         .agg(
                           count("passenger_count").as("percentage_3p"),
                           max("total_amount").as("max_amount_3p"),
                           min("total_amount").as("min_amount_3p")
                         )
    //для каждого дня посчитаем количество поездок с четырьмя и более пассажирами и определим
    // самую дешевую и самую дорогую поездку
    val moreCount = cleanData.filter("passenger_count >= 4")
                        .groupBy("date")
                        .agg(
                          count("passenger_count").as("percentage_4p_plus"),
                          max("total_amount").as("max_amount_4p"),
                          min("total_amount").as("min_amount_4p")
                        )
    //объединим все полученные таблицы
    var finalTable = totalCount.join(nullCount, Seq("date"))
                               .join(oneCount, Seq("date"))
                               .join(twoCount, Seq("date"))
                               .join(threeCount, Seq("date"))
                               .join(moreCount, Seq("date"))
    //рассчитаем процент поездок по количеству пассажиров, удалим столбец с общим количеством поездок
    // и отсортируем полученную таблицу по датам
    finalTable = finalTable.withColumn("percentage_zero", round(col("percentage_zero")*100/col("count"),1))
      .withColumn("percentage_1p", round(col("percentage_1p")*100/col("count"), 1))
      .withColumn("percentage_2p", round(col("percentage_2p")*100/col("count"),1))
      .withColumn("percentage_3p", round(col("percentage_3p")*100/col("count"), 1))
      .withColumn("percentage_4p_plus", round(col("percentage_4p_plus")*100/col("count"),1))
      .drop("count")
      .sort("date")

    finalTable.show(false)

    //запишем полученную таблицу в файл taxi.parquet
    finalTable.write.parquet("tmp/output/taxi.parquet")


  }
}