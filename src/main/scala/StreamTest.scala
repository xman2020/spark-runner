import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

object StreamTest {

  def main(args: Array[String]): Unit = {
    val ssc = SparkUtils.getStreamContext("StreamTest", args, 10)
    val stream = ssc.socketTextStream("localhost", 9999)

    // 没反应
    //val stream = ssc.textFileStream("src/main/resources/login_input")

    // 传统Stream不支持事件时间
    // 最小的精度1s

    // 命令：nc -lk 9999

    //this.print(stream)

    //this.writeFile(stream)

    //this.wordCount(stream)

    //this.loginCount(stream)

    //this.loginWindowCount(stream)

    this.loginCountSql(stream)

    ssc.start()
    ssc.awaitTermination()
  }

  def loginCountSql(stream: DStream[String]): Unit = {
    stream.print()

    stream
      //.window(Seconds(20), Seconds(10))
      .foreachRDD((rdd, time) => {
        val ss = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        import ss.implicits._

        val df = rdd.map(_.split(",")).map(x => Login(x(0), x(1), x(2))).toDF
        df.createOrReplaceTempView("login")

        ss.sql("select name, count(*) from login group by name").show()
        println(s"========= $time =========")
      })

    //-------------------------------------------
    //Time: 1548258370000 ms
    //-------------------------------------------
    //lh,2019-01-22 11:04:11,success
    //xs,2019-01-22 11:04:11,fail
    //dxd,2019-01-22 11:04:11,success
    //lh,2019-01-22 11:04:11,request
    //zp,2019-01-22 11:04:11,fail
    //xs,2019-01-22 11:04:11,request
    //dxd,2019-01-22 11:04:11,request
    //lh,2019-01-22 11:04:11,request
    //xs,2019-01-22 11:04:11,success
    //dxd,2019-01-22 11:04:11,success
    //...
    //+----+--------+
    //|name|count(1)|
    //+----+--------+
    //|  xs|       5|
    //|  zp|       2|
    //|  lh|       5|
    //| dxd|       6|
    //+----+--------+
    //========= 1548258370000 ms =========
  }

  case class Login(name: String, time: String, status: String)

  def loginWindowCount(stream: DStream[String]): Unit = {
    stream.print()

    stream
      .map(_.split(","))
      .map(x => (x(0), 1))
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(20), Seconds(10))
      .print()

    //-------------------------------------------
    //Time: 1548252020000 ms
    //-------------------------------------------
    //lh,2019-01-22 11:04:11,success
    //xs,2019-01-22 11:04:11,fail
    //dxd,2019-01-22 11:04:11,success
    //lh,2019-01-22 11:04:11,request
    //zp,2019-01-22 11:04:11,fail
    //-------------------------------------------
    //Time: 1548252020000 ms
    //-------------------------------------------
    //(dxd,1)
    //(lh,2)
    //(zp,1)
    //(xs,1)
    //
    //-------------------------------------------
    //Time: 1548252030000 ms
    //-------------------------------------------
    //xs,2019-01-22 11:04:11,request
    //zp,2019-01-22 11:04:11,fail
    //xs,2019-01-22 11:04:11,request
    //lh,2019-01-22 11:04:11,request
    //zp,2019-01-22 11:04:11,fail
    //xs,2019-01-22 11:04:11,success
    //hnx,2019-01-22 11:04:11,success
    //dxd,2019-01-22 11:04:11,request
    //csy,2019-01-22 11:04:11,success
    //-------------------------------------------
    //Time: 1548252030000 ms
    //-------------------------------------------
    //(dxd,2)
    //(lh,3)
    //(csy,1)
    //(hnx,1)
    //(zp,3)
    //(xs,4)
    //
    //-------------------------------------------
    //Time: 1548252040000 ms
    //-------------------------------------------
    //-------------------------------------------
    //Time: 1548252040000 ms
    //-------------------------------------------
    //(dxd,1)
    //(lh,1)
    //(csy,1)
    //(hnx,1)
    //(zp,2)
    //(xs,3)

    // 执行计划参见《Spark集锦》
  }

  def loginCount(stream: DStream[String]): Unit = {
    stream.print()

    stream
      .map(_.split(","))
      .filter(_ (2) == "success")
      .map(x => (x(0), 1))
      .reduceByKey(_ + _)
      .print()

    //-------------------------------------------
    //Time: 1548251290000 ms
    //-------------------------------------------
    //lh,2019-01-22 11:04:11,success
    //xs,2019-01-22 11:04:11,fail
    //dxd,2019-01-22 11:04:11,success
    //lh,2019-01-22 11:04:11,request
    //zp,2019-01-22 11:04:11,fail
    //xs,2019-01-22 11:04:11,request
    //dxd,2019-01-22 11:04:11,request
    //lh,2019-01-22 11:04:11,request
    //xs,2019-01-22 11:04:11,success
    //dxd,2019-01-22 11:04:11,success
    //...
    //
    //-------------------------------------------
    //Time: 1548251290000 ms
    //-------------------------------------------
    //(dxd,3)
    //(lh,2)
    //(csy,1)
    //(hnx,1)
    //(zp,1)
    //(xs,2)
  }

  def wordCount(stream: DStream[String]): Unit = {
    stream.print()

    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //-------------------------------------------
    //Time: 1548249890000 ms
    //-------------------------------------------
    //hello world
    //hello hadoop
    //hadoop is a bigdata platform
    //bigdata is a popular technic
    //
    //hadoop is good
    //
    //-------------------------------------------
    //Time: 1548249890000 ms
    //-------------------------------------------
    //(,1)
    //(hadoop,3)
    //(a,2)
    //(popular,1)
    //(good,1)
    //(is,3)
    //(hello,2)
    //(bigdata,2)
    //(world,1)
    //(technic,1)
    //...
    //
    //-------------------------------------------
  }

  def writeFile(stream: DStream[String]): Unit = {
    stream.saveAsTextFiles("src/main/resources/login_receive/login_receive")

    // 每隔10s生成一个文件夹，不管有没有数据，会产生好多文件夹
    // 有数据，在时间段内收一批一个文件
    //login_receive-1548237990000
    //  _SUCCESS
    //  part-00001
    //  part-00000
    // 无数据
    //login_receive-1548237970000
    //  _SUCCESS
  }

  def print(stream: DStream[String]): Unit = {
    stream.print()

    // 每隔10s打印一次
    //-------------------------------------------
    //Time: 1548236840000 ms
    //-------------------------------------------
    //lh,2019-01-22 11:04:11,request
    //xs,2019-01-22 11:04:11,fail
    //dxd,2019-01-22 11:04:11,success

    // 最多10行
  }

}
