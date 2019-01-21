import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object ScoreLargeTest {

  def main(args: Array[String]): Unit = {
    println(DateTime.now())

    val ss = SparkUtils.getSession("ScoreLargeTest", args)

    val df = readCsv(ss)

    df.createOrReplaceTempView("score")

    ss.sql("select * from score where name='csy'").show()

    println(DateTime.now())

    // 本机MacAir，20万条数据，查3条记录，12s
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

    ss.stop()
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
