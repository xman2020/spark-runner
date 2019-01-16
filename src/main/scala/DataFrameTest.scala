import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

object DataFrameTest {

  def main(args: Array[String]): Unit = {
    val ss = SparkUtils.getSession("DataFrameTest", args)
    import ss.implicits._

    //val df = readJson(ss)
    //val df = readTxt(ss)
    //val df = readTxt_(ss)
    val df = readCsv(ss)

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

    // +以下搞SparkSQL+------------------------------------------------------

    // 全局可用，新创建的会话也可以用，使用写法global_temp.score
    //df.createOrReplaceGlobalTempView("score")

    // 当前会话可用
    df.createOrReplaceTempView("score")

    println("ss.sql(\"select subject, avg(score) as avg_score from score group by subject order by avg_score desc\"):")
    ss.sql("select subject, avg(score) as avg_score from score group by subject order by avg_score desc").show()

    println("df-score:")
    df.show()

    val df2 = readJson2(ss)
    println("df2-student:")
    df2.show()

    df2.createOrReplaceTempView("student")

    println("ss.sql(\"select * from score sc join student st on sc.name = st.name where st.class = 3\"):")
    ss.sql("select * from score sc join student st on sc.name = st.name where st.class = 3").show()
    //+----+-----+-------+---+-----+----+------+
    //|name|score|subject|age|class|name|    no|
    //+----+-----+-------+---+-----+----+------+
    //|  lh| 67.0|chinese| 25|    3|  lh|123127|
    //|  lh| 92.0|   math| 25|    3|  lh|123127|
    //| dxd| 80.0|   math| 32|    3| dxd|123128|
    //| dxd| 85.5|chinese| 32|    3| dxd|123128|
    //+----+-----+-------+---+-----+----+------+

    val df3 = ss.sql("select st.name, st.class, st.no, sc.subject, sc.score " +
      "from score sc join student st on sc.name = st.name where st.class = 3")
    df3.show()
    // 保存时自动生成score2.json的目录，文件保存在该目录下
    df3.write.mode(SaveMode.Append).format("json").save("src/main/resources/score2.json")
    //{"name":"lh","class":"3","no":123127,"subject":"chines","score":67.0}
    //{"name":"lh","class":"3","no":123127,"subject":"math","score":92.0}
    //{"name":"dxd","class":"3","no":123128,"subject":"math","score":80.0}
    //{"name":"dxd","class":"3","no":123128,"subject":"chinese","score":85.5}
    println("Writing score2.txt is Finished")

    // 看源代码 type DataFrame = Dataset[Row] ，DataFrame就是DataSet
    val ds = df.as[Score]
    println("ds:")
    ds.show()

    ss.stop()
  }

  case class Score(name: String, subject: String, score: Double)

  def readTxt(ss: SparkSession): DataFrame = {
    import ss.implicits._

    val rdd = ss.sparkContext.textFile("src/main/resources/score.txt")
    val df = rdd.map(_.split(",")).map(x => Score(x(0), x(1), x(2).toDouble)).toDF

    df
  }

  def readTxt_(ss: SparkSession): DataFrame = {
    val rdd = ss.sparkContext.textFile("src/main/resources/score.txt")
    val rdd2 = rdd.map(_.split(",")).map(x => Row(x(0), x(1), x(2).toDouble))

    val schema = StructType(Array(
      StructField("name", StringType),
      StructField("subject", StringType),
      StructField("score", DoubleType)))

    val df = ss.createDataFrame(rdd2, schema)

    df
  }

  def readCsv(ss: SparkSession): DataFrame = {
    val df = ss.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/score.csv")

    df
  }

  def readJson(ss: SparkSession): DataFrame = {
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

    val df = ss.read.json("src/main/resources/score.json")

    df
  }

  def readJson2(ss: SparkSession): DataFrame = {
    val df = ss.read.json("src/main/resources/student.json")

    df
  }

}
