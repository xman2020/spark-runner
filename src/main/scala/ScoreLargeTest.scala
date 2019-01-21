import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

object ScoreLargeTest {

  def main(args: Array[String]): Unit = {
    val ss = SparkUtils.getSession("ScoreLargeTest", args)

    //this.selectCsv(ss)

    //this.csvToParquet(ss)

    this.selectParquet(ss)

    ss.stop()
  }

  def selectParquet(ss: SparkSession):Unit ={
    println("Select Parquet:")
    println(DateTime.now())

    val df = ss.read.parquet("src/main/resources/score_large.parquet")
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
    df.write.mode(SaveMode.Overwrite).parquet("src/main/resources/score_large.parquet")

    println(DateTime.now())

    //2019-01-21T23:15:50.616+08:00
    //2019-01-21T23:16:00.208+08:00

    //文件大小，CSV：2,875,456 bytes，Parquet：11,713 bytes，只是原来的千分之4，灰常节约空间
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
      .csv("src/main/resources/score_large.csv")

    df
  }

}
