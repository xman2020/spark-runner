import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StructStreamTest {

  def main(args: Array[String]): Unit = {
    val ss = SparkUtils.getSession("StructStreamTest", args)
    val stream = ss.readStream.format("socket").option("host", "localhost").option("port", "9999").load()

    // 与传统Stream区别，这里得到是一个DataFrame

    //this.output(stream)

    //this.wordCount(stream)

    this.loginCount(stream)

    //this.loginCountSql(stream)

    //this.loginCountSql2(ss)



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
    val query = result.writeStream.outputMode(mode).format("console")
      .trigger(Trigger.ProcessingTime(1000)).start()

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

    // 不能用，运行报错
    // Exception in thread "main" org.apache.spark.sql.AnalysisException: The socket source does not support a user-specified schema.;

    df.createOrReplaceTempView("login")
    val df2 = ss.sql("select name, count(*) from login group by name")

    this.output("complete", df2)
  }
  
}
