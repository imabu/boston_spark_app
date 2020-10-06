package com.github.boston.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

object BostonCrimesMap {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("Crimes aggregate")
        .getOrCreate()

    import spark.implicits._

    val crimeCsvPath = args(0)
    val offenseCodesCsvPath = args(1)
    val outputDir = args(2)

    val frequent_crime_types_top_N = 3

    val crime = spark.read.format("csv").option("header", "true").load(crimeCsvPath)
    val offenseCodes = spark.read.format("csv").option("header", "true").load(offenseCodesCsvPath)

    val crime_with_offence_name = crime.join(broadcast(offenseCodes), 'OFFENSE_CODE.equalTo('CODE), "left")

    crime_with_offence_name
      .groupBy('DISTRICT, 'MONTH, 'YEAR)
      .agg(count('INCIDENT_NUMBER) alias ("crimes_total_monthly"),
        sum('Lat) alias ("lat_sum_monthly"), sum('Long) alias ("lng_sum_monthly"))
      .createOrReplaceTempView("crimes_monthly")

    val crimes_agg_tmp = spark.sql("select DISTRICT,  " +
      "percentile_approx(crimes_total_monthly, 0.5) as crimes_monthly," +
      "sum(crimes_total_monthly) as crimes_total, " +
      "sum(lat_sum_monthly) as lat_sum, " +
      "sum(lng_sum_monthly) as lng_sum " +
      "from crimes_monthly group by DISTRICT")

    val frequent_crime_types = crime_with_offence_name
      .withColumn("crime_type", functions.split('NAME, "-").getItem(0))
      .groupBy('DISTRICT, 'crime_type)
      .agg(count('INCIDENT_NUMBER) alias ("crimes_by_crime_type"))
      .withColumn("crimes_by_crime_type_rank",
        row_number() over (Window.partitionBy('DISTRICT, 'crime_type) orderBy ('crimes_by_crime_type).desc))
      .filter('crimes_by_crime_type_rank.leq(lit(frequent_crime_types_top_N)))
      .orderBy('DISTRICT, 'crimes_by_crime_type_rank)
      .groupBy('DISTRICT)
      .agg(concat_ws(", ", collect_list('crime_type)) alias ("frequent_crime_types"))

    val result = crimes_agg_tmp.join(frequent_crime_types, Seq("DISTRICT"), "left")
      .withColumn("lat", 'lat_sum/'crimes_total)
      .withColumn("lng", 'lng_sum/'crimes_total)
      .drop("lat_sum", "lng_sum")
    result.write.mode(SaveMode.Overwrite).format("parquet").save(outputDir)
  }

}

