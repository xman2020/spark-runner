import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object StructStreamTest {

  def main(args: Array[String]): Unit = {
    val ss = SparkUtils.getSession("StructStreamTest", args)
    val stream = ss.readStream.format("socket").option("host", "localhost").option("port", "9999").load()

    // 与传统Stream区别，这里得到是一个DataFrame

    //this.output(stream)

    //this.wordCount(stream)

    //this.loginCount(stream)

    //this.loginCountSql(stream)

    //this.loginCountSql2(ss)

    //this.loginCountWindow(stream)

    this.loginCountWindow2(stream)

    //this.loginCountWindowSql(stream)

    //this.loginCountWatermark(stream)
  }

  def loginCountWatermark(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
    val df2 = df.withColumn("timestamp", functions.to_timestamp($"time", "yyyy-MM-dd HH:mm:ss"))
      .withWatermark("timestamp", "10 seconds")
      .groupBy(functions.window($"timestamp", "30 seconds"),
        $"name").count()

    this.output("update", df2)

    // 一行一行输入
    //lh,2019-01-28 16:34:40,success
    //lh,2019-01-28 16:34:29,success
    //lh,2019-01-28 16:34:30,success
    //-------------------------------------------
    //Batch: 0
    //-------------------------------------------
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-01-28 16:34:30, 2019-01-28 16:35:00]|lh  |1    |
    //+------------------------------------------+----+-----+
    //
    //19/02/26 15:19:29 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 11078 milliseconds
    //
    //-------------------------------------------
    //Batch: 1
    //-------------------------------------------
    //+------+----+-----+
    //|window|name|count|
    //+------+----+-----+
    //+------+----+-----+
    //
    //19/02/26 15:19:42 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 6385 milliseconds
    //
    //-------------------------------------------
    //Batch: 2
    //-------------------------------------------
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-01-28 16:34:30, 2019-01-28 16:35:00]|lh  |2    |
    //+------------------------------------------+----+-----+
    //
    //19/02/26 15:20:06 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 6120 milliseconds

    // 虽然第二次的数据丢弃了，但是还是有一条空输出
  }

  def loginCountWindowSql(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
    df.createOrReplaceTempView("login")
    val df2 = stream.sparkSession.sql("select name, window(time, '30 seconds'), count(*) " +
      "from login group by name, window(time, '30 seconds')")

    this.output("complete", df2)

    // complete、update模式都正常
    // append报错
    // Exception in thread "main" org.apache.spark.sql.AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark

    // 一行一行输入
    //lh,2019-01-28 16:34:40,success
    //lh,2019-01-28 16:34:40,success
    //dxd,2019-02-01 08:21:08,success
    //-------------------------------------------
    //Batch: 0
    //-------------------------------------------
    //+----+------------------------------------------+--------+
    //|name|window                                    |count(1)|
    //+----+------------------------------------------+--------+
    //|lh  |[2019-01-28 16:34:30, 2019-01-28 16:35:00]|1       |
    //+----+------------------------------------------+--------+
    //
    //19/01/29 11:00:16 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 13901 milliseconds
    //
    //-------------------------------------------
    //Batch: 1
    //-------------------------------------------
    //+----+------------------------------------------+--------+
    //|name|window                                    |count(1)|
    //+----+------------------------------------------+--------+
    //|lh  |[2019-01-28 16:34:30, 2019-01-28 16:35:00]|2       |
    //+----+------------------------------------------+--------+
    //
    //19/01/29 11:00:29 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 6113 milliseconds
    //
    //-------------------------------------------
    //Batch: 2
    //-------------------------------------------
    //+----+------------------------------------------+--------+
    //|name|window                                    |count(1)|
    //+----+------------------------------------------+--------+
    //|dxd |[2019-02-01 08:21:00, 2019-02-01 08:21:30]|1       |
    //|lh  |[2019-01-28 16:34:30, 2019-01-28 16:35:00]|2       |
    //+----+------------------------------------------+--------+
    //
    //19/01/29 11:01:11 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 6253 milliseconds
  }

  def loginCountWindow2(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
    val df2 = df.groupBy(functions.window($"time", "30 seconds"),
      $"name").count()

    this.output("update", df2)

    // 一行一行输入
    //lh,2019-01-28 16:34:40,success
    //lh,2019-01-28 16:34:40,success
    //dxd,2019-02-01 08:21:08,success
    //-------------------------------------------
    //Batch: 0
    //-------------------------------------------
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-01-28 16:34:30, 2019-01-28 16:35:00]|lh  |1    |
    //+------------------------------------------+----+-----+
    //
    //19/01/29 10:50:54 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 10581 milliseconds
    //
    //-------------------------------------------
    //Batch: 1
    //-------------------------------------------
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-01-28 16:34:30, 2019-01-28 16:35:00]|lh  |2    |
    //+------------------------------------------+----+-----+
    //
    //19/01/29 10:51:05 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 6943 milliseconds
    //
    //-------------------------------------------
    //Batch: 2
    //-------------------------------------------
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-02-01 08:21:00, 2019-02-01 08:21:30]|dxd |1    |
    //+------------------------------------------+----+-----+
    //
    //19/01/29 10:51:15 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 5353 milliseconds

    // 测试同一数据，在不同精度下的表现
    //lh,2019-02-26 16:29:37,success
    //
    // 1 seconds
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-02-26 16:29:37, 2019-02-26 16:29:38]|lh  |1    |
    //+------------------------------------------+----+-----+
    //
    // 1 minutes
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-02-26 16:29:00, 2019-02-26 16:30:00]|lh  |1    |
    //+------------------------------------------+----+-----+
    //
    // 1 hours
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-02-26 16:00:00, 2019-02-26 17:00:00]|lh  |1    |
    //+------------------------------------------+----+-----+
    //
    // 1 days
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-02-26 08:00:00, 2019-02-27 08:00:00]|lh  |1    |
    //+------------------------------------------+----+-----+
    // 为什么是8点，是否可以调整

    // 1 months
    // 启动报错：Exception in thread "main" java.lang.IllegalArgumentException: Intervals greater than a month is not supported (1 months).
    // month不支持

    // 优化配置，spark.sql.shuffle.partitions=4（原来200），见SparkUtils
    // 原来第一次执行：10581 milliseconds
    // 现在第一次执行：4841 milliseconds
    // 比原来快多了

  }

  def loginCountWindow(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
    val df2 = df.groupBy(functions.window($"time", "30 seconds", "10 seconds"),
      $"name").count()

    this.output("complete", df2)

    // 一行一行输入
    //lh,2019-01-28 16:34:40,success
    //xs,2019-01-28 16:35:41,success
    //xs,2019-01-28 16:35:41,success
    //lh,2019-01-29 16:35:41,success
    //-------------------------------------------
    //Batch: 0
    //-------------------------------------------
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-01-28 16:34:20, 2019-01-28 16:34:50]|lh  |1    |
    //|[2019-01-28 16:34:30, 2019-01-28 16:35:00]|lh  |1    |
    //|[2019-01-28 16:34:40, 2019-01-28 16:35:10]|lh  |1    |
    //+------------------------------------------+----+-----+
    //
    //19/01/28 17:02:16 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 10392 milliseconds
    //
    //-------------------------------------------
    //Batch: 1
    //-------------------------------------------
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-01-28 16:34:20, 2019-01-28 16:34:50]|lh  |1    |
    //|[2019-01-28 16:34:30, 2019-01-28 16:35:00]|lh  |1    |
    //|[2019-01-28 16:34:40, 2019-01-28 16:35:10]|lh  |1    |
    //|[2019-01-28 16:35:40, 2019-01-28 16:36:10]|xs  |1    |
    //|[2019-01-28 16:35:20, 2019-01-28 16:35:50]|xs  |1    |
    //|[2019-01-28 16:35:30, 2019-01-28 16:36:00]|xs  |1    |
    //+------------------------------------------+----+-----+
    //
    //19/01/28 17:02:39 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 5660 milliseconds
    //
    //-------------------------------------------
    //Batch: 2
    //-------------------------------------------
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-01-28 16:34:20, 2019-01-28 16:34:50]|lh  |1    |
    //|[2019-01-28 16:34:30, 2019-01-28 16:35:00]|lh  |1    |
    //|[2019-01-28 16:34:40, 2019-01-28 16:35:10]|lh  |1    |
    //|[2019-01-28 16:35:40, 2019-01-28 16:36:10]|xs  |2    |
    //|[2019-01-28 16:35:20, 2019-01-28 16:35:50]|xs  |2    |
    //|[2019-01-28 16:35:30, 2019-01-28 16:36:00]|xs  |2    |
    //+------------------------------------------+----+-----+
    //
    //19/01/28 17:04:20 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 6303 milliseconds
    //
    //-------------------------------------------
    //Batch: 3
    //-------------------------------------------
    //+------------------------------------------+----+-----+
    //|window                                    |name|count|
    //+------------------------------------------+----+-----+
    //|[2019-01-29 16:35:20, 2019-01-29 16:35:50]|lh  |1    |
    //|[2019-01-29 16:35:30, 2019-01-29 16:36:00]|lh  |1    |
    //|[2019-01-28 16:34:20, 2019-01-28 16:34:50]|lh  |1    |
    //|[2019-01-28 16:34:30, 2019-01-28 16:35:00]|lh  |1    |
    //|[2019-01-29 16:35:40, 2019-01-29 16:36:10]|lh  |1    |
    //|[2019-01-28 16:34:40, 2019-01-28 16:35:10]|lh  |1    |
    //|[2019-01-28 16:35:40, 2019-01-28 16:36:10]|xs  |2    |
    //|[2019-01-28 16:35:20, 2019-01-28 16:35:50]|xs  |2    |
    //|[2019-01-28 16:35:30, 2019-01-28 16:36:00]|xs  |2    |
    //+------------------------------------------+----+-----+
    //
    //19/01/28 17:05:02 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 1000 milliseconds, but spent 5660 milliseconds
  }


  def loginCountSql(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
    df.createOrReplaceTempView("login")
    val df2 = stream.sparkSession.sql("select name, count(*) from login group by name")

    this.output("complete", df2)

    // 输入
    // 第一批
    //lh,2019-01-22 11:04:11,success
    //xs,2019-01-22 11:04:11,fail
    //dxd,2019-01-22 11:04:11,success
    //lh,2019-01-22 11:04:11,request
    //
    // 第二批
    //xs,2019-01-22 11:04:11,request
    //dxd,2019-01-22 11:04:11,request
    //lh,2019-01-22 11:04:11,request
    //xs,2019-01-22 11:04:11,success
    //
    // 输出
    //-------------------------------------------
    //Batch: 0
    //-------------------------------------------
    //+----+--------+
    //|name|count(1)|
    //+----+--------+
    //|  xs|       1|
    //|  lh|       2|
    //| dxd|       1|
    //+----+--------+
    //
    //19/01/24 23:39:32 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 100 milliseconds, but spent 11247 milliseconds
    //
    //-------------------------------------------
    //Batch: 1
    //-------------------------------------------
    //+----+--------+
    //|name|count(1)|
    //+----+--------+
    //|  xs|       3|
    //|  lh|       3|
    //| dxd|       2|
    //+----+--------+
    //
    //19/01/24 23:40:19 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 100 milliseconds, but spent 6011 milliseconds

    // 一般第一次会比较慢，后续比第一次快，但也还是慢
    // 400条数据测试，处理要5s左右，4040看Task有200个
  }

  def loginCount(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String]
      .map(_.split(","))
      .map(x => (x(0), 1))
      .groupBy("_1")
      .count()

    this.output("complete", df)

    // 同SQL执行的现象，处理慢，Task多（200个）
  }

  def wordCount(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val words = stream.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    // show报错，不能用，只能output出去
    //wordCounts.show()

    this.output("complete", wordCounts)

    // 输入
    // i am a good boy
    // i am a good man
    //
    // 输出
    //-------------------------------------------
    //Batch: 0
    //-------------------------------------------
    //+-----+-----+
    //|value|count|
    //+-----+-----+
    //|  boy|    1|
    //|    i|    1|
    //| good|    1|
    //|    a|    1|
    //|   am|    1|
    //+-----+-----+
    //
    //19/01/24 23:00:00 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 100 milliseconds, but spent 11460 milliseconds
    //
    //-------------------------------------------
    //Batch: 1
    //-------------------------------------------
    //+-----+-----+
    //|value|count|
    //+-----+-----+
    //|  boy|    1|
    //|  man|    1|
    //|    i|    2|
    //| good|    2|
    //|    a|    2|
    //|   am|    2|
    //+-----+-----+
    //
    //19/01/24 23:00:22 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 100 milliseconds, but spent 7671 milliseconds

    // 计算结果会与历史的累计
  }

  def output(stream: DataFrame): Unit = {
    this.output("update", stream)

    // 输入
    // i am a good boy
    // i am a good man
    //
    // 输出
    //-------------------------------------------
    //Batch: 0
    //-------------------------------------------
    //+---------------+
    //|          value|
    //+---------------+
    //|i am a good boy|
    //+---------------+
    //
    //19/01/24 22:48:41 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 100 milliseconds, but spent 2185 milliseconds
    //
    //-------------------------------------------
    //Batch: 1
    //-------------------------------------------
    //+---------------+
    //|          value|
    //+---------------+
    //|i am a good man|
    //+---------------+
    //
    //19/01/24 22:49:06 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 100 milliseconds, but spent 279 milliseconds

    // update模式正常运行
    // complete模式下，启动报错
    // Exception in thread "main" org.apache.spark.sql.AnalysisException: Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;;
  }

  def output(mode: String, result: DataFrame): Unit = {
    val query = result.writeStream.outputMode(mode).format("console").option("truncate", "false")
      .trigger(Trigger.ProcessingTime(1000)).start()

    // 输出到Text文件有待摸索
    //    val query = result.writeStream.outputMode(mode).format("text")
    //      .option("checkpointLocation", "src/main/resources/checkpoint")
    //      .option("path", "src/main/resources/struct_stream_out")

    // 10ms告警，搞不定，改为100ms
    //19/01/24 21:46:17 WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 10 milliseconds, but spent 12 milliseconds

    query.awaitTermination()
  }

  @deprecated
  def loginCountSql2(ss: SparkSession): Unit = {
    val schema = StructType(Array(
      StructField("name", StringType),
      StructField("time", StringType),
      StructField("status", StringType)))

    val df = ss.readStream.format("socket").option("host", "localhost").option("port", "9999")
      .option("sep", ",").schema(schema).load()

    // 该方法运行报错
    // Exception in thread "main" org.apache.spark.sql.AnalysisException: The socket source does not support a user-specified schema.;

    df.createOrReplaceTempView("login")
    val df2 = ss.sql("select name, count(*) from login group by name")

    this.output("complete", df2)
  }

}
