import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

object ScoreLargeTest {

  def main(args: Array[String]): Unit = {
    val ss = SparkUtils.getSession("ScoreLargeTest", args)

    //this.selectCsv(ss)

    //this.csvToParquet(ss)

    //this.selectParquet(ss)

    //this.csvToParquet1000w(ss)

    //this.selectParquet1000w(ss)

    //this.parquet1000wTo1y(ss)

    this.selectParquet1y(ss)

    ss.stop()
  }

  // 总结:
  // 1、数据量增加(20w、1000w、1y)，时间增加不明显，10s左右
  // 2、where查询、count统计时间差不多

  def selectParquet1y(ss: SparkSession): Unit = {
    println("Select Parquet 1y:")
    println(DateTime.now())

    val df = ss.read.parquet("src/main/resources/score_1y_parquet")
    df.createOrReplaceTempView("score")
    //ss.sql("select * from score where name='csy'").show()

    // 1亿条只要11s
    //2019-01-22T11:04:11.826+08:00
    //+----+-------+-----+
    //|name|subject|score|
    //+----+-------+-----+
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //+----+-------+-----+
    //only showing top 20 rows
    //
    //2019-01-22T11:04:22.209+08:00

    //ss.sql("select count(*) from score").show()

    //2019-01-22T11:06:00.259+08:00
    //+---------+
    //| count(1)|
    //+---------+
    //|104833500|
    //+---------+
    //
    //2019-01-22T11:06:07.113+08:00

    //println(DateTime.now())

    ss.sql("select name, count(*) from score group by name").show()

    //2019-01-22T11:06:52.497+08:00
    //+----+--------+
    //|name|count(1)|
    //+----+--------+
    //|  xs|29952000|
    //|  zp|14976000|
    //| csy|    1500|
    //|  lh|29952000|
    //| dxd|29952000|
    //+----+--------+
    //
    //2019-01-22T11:07:04.384+08:00

    println(DateTime.now())
  }

  def selectParquet1000w(ss: SparkSession): Unit = {
    println("Select Parquet 1000w:")
    println(DateTime.now())

    val df = ss.read.parquet("src/main/resources/score_1000w_parquet")
    df.createOrReplaceTempView("score")
    //ss.sql("select * from score where name='csy'").show()

    // 1000w条也是7s
    //2019-01-22T09:20:15.756+08:00
    //+----+-------+-----+
    //|name|subject|score|
    //+----+-------+-----+
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //+----+-------+-----+
    //only showing top 20 rows
    //
    //2019-01-22T09:20:22.754+08:00

    ss.sql("select name, count(*) from score group by name").show()

    //聚合稍微慢一些，要10s
    //2019-01-22T09:22:44.604+08:00
    //+----+--------+
    //|name|count(1)|
    //+----+--------+
    //|  xs| 2995200|
    //|  zp| 1497600|
    //| csy|     150|
    //|  lh| 2995200|
    //| dxd| 2995200|
    //+----+--------+
    //
    //2019-01-22T09:22:54.791+08:00

    println(DateTime.now())
  }

  def selectParquet(ss: SparkSession): Unit = {
    println("Select Parquet:")
    println(DateTime.now())

    val df = ss.read.parquet("src/main/resources/score_20w_parquet")
    df.createOrReplaceTempView("score")
    //ss.sql("select * from score where name='csy'").show()
    ss.sql("select count(*) from score").show()

    println(DateTime.now())

    // 只要7s，比CSV（12s）快多啦
    //2019-01-21T23:22:18.721+08:00
    //+----+-------+-----+
    //|name|subject|score|
    //+----+-------+-----+
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //+----+-------+-----+
    //
    //2019-01-21T23:22:25.496+08:00

    // 6s多
    //2019-01-21T23:28:38.123+08:00
    //+--------+
    //|count(1)|
    //+--------+
    //|  209667|
    //+--------+
    //
    //2019-01-21T23:28:44.423+08:00

    // 两个SQL还是7s，说明有优化器
    //2019-01-21T23:27:23.688+08:00
    //+----+-------+-----+
    //|name|subject|score|
    //+----+-------+-----+
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //+----+-------+-----+
    //
    //+--------+
    //|count(1)|
    //+--------+
    //|  209667|
    //+--------+
    //
    //2019-01-21T23:27:30.788+08:00
  }

  def csvToParquet(ss: SparkSession): Unit = {
    println("CSV to parquet:")
    println(DateTime.now())

    val df = readCsv(ss)
    df.write.mode(SaveMode.Overwrite).parquet("src/main/resources/score_20w_parquet")

    println(DateTime.now())

    //2019-01-21T23:15:50.616+08:00
    //2019-01-21T23:16:00.208+08:00

    //文件大小，CSV：2,875,456 bytes，Parquet：11,713 bytes，只是原来的千分之4，灰常节约空间
  }

  def csvToParquet1000w(ss: SparkSession): Unit = {
    println("CSV to parquet 1000w:")
    println(DateTime.now())

    val df = readCsv(ss)

    for (_ <- 1 to 50) {
      df.write.mode(SaveMode.Append).parquet("src/main/resources/score_1000w_parquet")
    }

    println(DateTime.now())

    //2019-01-22T09:16:04.327+08:00
    //2019-01-22T09:16:54.241+08:00

    //文件大小：590,650 bytes
  }

  def parquet1000wTo1y(ss: SparkSession): Unit = {
    println("Parquet 1000w to 1y:")
    println(DateTime.now())

    val df = ss.read.parquet("src/main/resources/score_1000w_parquet")

    for (_ <- 1 to 10) {
      df.write.mode(SaveMode.Append).parquet("src/main/resources/score_1y_parquet")
    }

    println(DateTime.now())

    //1亿比1000万还要快
    //2019-01-22T09:56:14.060+08:00
    //2019-01-22T09:57:01.630+08:00

    //文件大小：5,833,230 bytes
  }

  def selectCsv(ss: SparkSession): Unit = {
    println("Select CSV:")
    println(DateTime.now())

    val df = readCsv(ss)
    df.createOrReplaceTempView("score")
    ss.sql("select * from score where name='csy'").show()

    println(DateTime.now())

    // 本机MacAir，20万条数据，查3条记录，耗时12s
    //2019-01-21T19:32:59.818+08:00
    //+----+-------+-----+
    //|name|subject|score|
    //+----+-------+-----+
    //| csy|chinese| 92.5|
    //| csy|   math| 88.0|
    //| csy|english| 86.5|
    //+----+-------+-----+
    //
    //2019-01-21T19:33:11.245+08:00
  }

  def readCsv(ss: SparkSession): DataFrame = {
    val df = ss.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/score_20w.csv")

    df
  }

}
