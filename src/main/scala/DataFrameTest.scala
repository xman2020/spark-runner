import org.apache.spark.sql.SparkSession

object DataFrameTest {

  def main(args: Array[String]): Unit = {
    val ss = SparkUtils.getSession("DataFrameTest", args)
    import ss.implicits._

    val df = readJson(ss)

    // show报错
    //    Exception in thread "main" org.apache.spark.sql.AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
    //      referenced columns only include the internal corrupt record column
    //    (named _corrupt_record by default). For example:
    //      spark.read.schema(schema).json(file).filter($"_corrupt_record".isNotNull).count()
    //    and spark.read.schema(schema).json(file).select("_corrupt_record").show().
    //      Instead, you can cache or save the parsed results and then send the same query.
    //    For example, val df = spark.read.schema(schema).json(file).cache() and then
    //    df.filter($"_corrupt_record".isNotNull).count().;
    // 原因是json文件的score字段没有加""

    println("df:")
    df.show()
    //+----+-----+-------+
    //|name|score|subject|
    //+----+-----+-------+
    //| dxd| 85.5|chinese|
    //|  lh| 92.0|   math|
    //|  xs| 98.5|english|
    //| dxd| 80.0|   math|
    //|  lh| 67.0|chinese|
    //|  zp| 75.0|english|
    //|  xs| 79.5|   math|
    //+----+-----+-------+

    println("schema:")
    df.printSchema()
    //root
    // |-- name: string (nullable = true)
    // |-- score: double (nullable = true)
    // |-- subject: string (nullable = true)

    println("df.select(\"name\"):")
    df.select("name").show()

    println(" df.select(\"name\").distinct():")
    df.select("name").distinct().show()

    println("df.where(\"score > 80\"):")
    df.where("score > 80").show()

    println("df.sort($\"score\".desc):")
    df.sort($"score".desc).show()

    println("df.groupBy(\"name\").sum(\"score\"):")
    df.groupBy("name").sum("score").show()

    println("df.groupBy(\"score\").avg(\"score\"):")
    df.groupBy("subject").avg("score").show()
    //+-------+-----------------+
    //|subject|       avg(score)|
    //+-------+-----------------+
    //|chinese|            76.25|
    //|english|            86.75|
    //|   math|83.83333333333333|
    //+-------+-----------------+

    // 全局可用，新创建的会话也可以用
    df.createOrReplaceGlobalTempView("score")

    // 当前会话可用
    //df.createOrReplaceTempView("score")

    println("ss.sql(\"select subject, avg(score) as avg_score from global_temp.score group by subject order by avg_score desc\"):")
    ss.sql("select subject, avg(score) as avg_score from global_temp.score group by subject order by avg_score desc").show()

    ss.stop()
  }

  def readJson(ss: SparkSession) = {
    // read.json报错
    //    Exception in thread "main" java.lang.IllegalArgumentException: Illegal pattern component: XXX
    //    at org.apache.commons.lang3.time.FastDateFormat.parsePattern(FastDateFormat.java:577)
    //    at org.apache.commons.lang3.time.FastDateFormat.init(FastDateFormat.java:444)
    // pom文件添加以下后正常
    //    <dependency>
    //      <groupId>org.apache.commons</groupId>
    //      <artifactId>commons-lang3</artifactId>
    //      <version>3.5</version>
    //    </dependency>

    val df = ss.read.json("/Users/chenshuyuan/Desktop/project/bigdata/spark-runner/src/main/resources/score.json")
    df
  }

}
