import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetTest {

  def main(args: Array[String]): Unit = {
    val ss = SparkUtils.getSession("ParquetTest", args)

    // Json文件内容写入Parquet文件
    this.jsonToParquet(ss)

    var path = "src/main/resources/score.parquet"
    var table = "score"
    this.readParquet(ss, path, table, "select * from score")
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

    // Java对象写入Parquet文件
    this.objToParquet(ss)

    this.readParquet(ss, path, table, "select * from score")
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
    //|  zp| 92.5|chinese|
    //| csy| 86.5|   math|
    //+----+-----+-------+

    // Json文件内容写入分区的Parquet文件
    this.jsonToPartitionedParquet(ss)

    path = "src/main/resources/score_parquet"
    this.readParquet(ss, path, table, "select * from score")
    //+----+-----+-------+
    //|name|score|subject|
    //+----+-----+-------+
    //| dxd| 85.5|chinese|
    //|  lh| 67.0|chinese|
    //|  xs| 98.5|english|
    //|  zp| 75.0|english|
    //|  lh| 92.0|   math|
    //| dxd| 80.0|   math|
    //|  xs| 79.5|   math|
    //+----+-----+-------+

    ss.stop()
  }

  def objToParquet(ss: SparkSession): Unit = {
    val df = ss.createDataFrame(Array(
      Score("csy", "math", 86.5),
      Score("zp", "chinese", 92.5)))
    df.write.mode(SaveMode.Append).parquet("src/main/resources/score.parquet")
    println("score obj write to score.parquet is finished!")
    df.show()
  }

  def jsonToParquet(ss: SparkSession): Unit = {
    val df = DataFrameTest.readJson(ss)
    df.write.mode(SaveMode.Overwrite).parquet("src/main/resources/score.parquet")
    println("score.json write to score.parquet is finished!")
    println()
  }

  def jsonToPartitionedParquet(ss: SparkSession): Unit = {
    val df = DataFrameTest.readJson(ss)
    df.where("subject='chinese'").write.mode(SaveMode.Overwrite).parquet("src/main/resources/score_parquet/subject=chinese/score.parquet")
    df.where("subject='math'").write.mode(SaveMode.Overwrite).parquet("src/main/resources/score_parquet/subject=math/score.parquet")
    df.where("subject='english'").write.mode(SaveMode.Overwrite).parquet("src/main/resources/score_parquet/subject=english/score.parquet")
    println("score.json write to partitioned score.parquet is finished!")
    println()
  }

  def readParquet(ss: SparkSession, path: String, table: String, sql: String): Unit = {
    val df = ss.read.parquet(path)
    df.createOrReplaceTempView(table)
    println("path=" + path)
    println("sql=" + sql)
    ss.sql(sql).show()
  }

}
