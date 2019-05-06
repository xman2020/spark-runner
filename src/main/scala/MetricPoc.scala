import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, functions}

object MetricPoc {

  def main(args: Array[String]): Unit = {
    val ss = SparkUtils.getSession("MetricPoc", args)
    val stream = ss.readStream.format("socket").option("host", "localhost").option("port", "9999").load()

    // 与传统Stream区别，这里得到是一个DataFrame

    //this.output(stream)

    //this.wordCount(stream)

    //this.loginCount(stream)

    //this.loginCountSql(stream)

    //this.loginCountSql2(ss)

    //this.loginCountWindow(stream)

    //this.loginCountWindow2(stream)

    this.loginCountWindowSql(stream)

    //this.loginCountWatermark(stream)
  }

  def loginCountWatermark(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
    val df2 = df.withColumn("timestamp", functions.to_timestamp($"time", "yyyy-MM-dd HH:mm:ss"))
      .withWatermark("timestamp", "10 seconds")
      //.groupBy(functions.window($"timestamp", "30 seconds"),
      //  $"name").count()

    //lh,2019-01-28 16:34:40,success
    //lh,2019-01-28 16:34:29,success
    //lh,2019-01-28 16:34:30,success

    df2.createOrReplaceTempView("login")

    val df3 = df2.sparkSession.sql("select name, window(time, '30 seconds'), count(*) " +
      "from login group by name, window(time, '30 seconds')")

    //ToFix
    //问题：这种用法Watermark不起作用

    this.output("update", df3)
  }

  def loginCountWindowSql(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
    df.createOrReplaceTempView("login")
    val df2 = stream.sparkSession.sql("select name, window(time, '30 seconds'), count(*) " +
      "from login group by name, window(time, '30 seconds')")

    val df3 = stream.sparkSession.sql("select * from login ")

    //ToFix
    //问题：df3没有作用

    this.output("update", df2)
    this.output("update", df3)
  }

  def loginCountWindow2(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
    val df2 = df.groupBy(functions.window($"time", "30 seconds"),
      $"name").count()

    this.output("update", df2)
  }

  def loginCountWindow(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
    val df2 = df.groupBy(functions.window($"time", "30 seconds", "10 seconds"),
      $"name").count()

    this.output("complete", df2)
  }


  def loginCountSql(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
    df.createOrReplaceTempView("login")
    val df2 = stream.sparkSession.sql("select name, count(*) from login group by name")

    this.output("complete", df2)
  }

  def loginCount(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String]
      .map(_.split(","))
      .map(x => (x(0), 1))
      .groupBy("_1")
      .count()

    this.output("complete", df)
  }

  def wordCount(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val words = stream.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    this.output("complete", wordCounts)
  }

  def output(stream: DataFrame): Unit = {
    this.output("update", stream)
  }

  def output(mode: String, result: DataFrame): Unit = {
    val query = result.writeStream.outputMode(mode).format("console").option("truncate", "false")
      .trigger(Trigger.ProcessingTime(1000)).start()

    query.awaitTermination()
  }

}
