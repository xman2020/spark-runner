import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

object ClPoc {

  def main(args: Array[String]): Unit = {
    val ss = SparkUtils.getSession("ClPoc", args)
    val stream = ss.readStream.format("socket").option("host", "localhost").option("port", "9999").load()

    //this.loanMetric1(stream)

    this.loanMetric2(stream)

  }

  def loanMetric1(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(","))
      .map(x => LoanApply(x(0), x(1), x(2), x(3))).toDF

    df.createOrReplaceTempView("loan_apply")

    val sql =
      """
        select window(applTime, '1 seconde') as datetime, realtelNo, count(*) as optTimes
          from loan_apply
      group by window(applTime, '1 seconde'), realtelNo
      """
    val df2 = stream.sparkSession.sql(sql)

    //123,lh,13851851801,2018-01-29 16:35:20
    //124,nx,13851851802,2018-01-29 16:35:21
    //125,lh,13851851801,2018-01-29 16:35:20

    // 1 seconds
    //+------------------------------------------+-----------+--------+
    //|datetime                                  |realtelNo  |optTimes|
    //+------------------------------------------+-----------+--------+
    //|[2018-01-29 16:35:21, 2018-01-29 16:35:22]|13851851802|1       |
    //|[2018-01-29 16:35:20, 2018-01-29 16:35:21]|13851851801|2       |
    //+------------------------------------------+-----------+--------+

    // 1 minutes
    //+------------------------------------------+-----------+--------+
    //|datetime                                  |realtelNo  |optTimes|
    //+------------------------------------------+-----------+--------+
    //|[2018-01-29 16:35:00, 2018-01-29 16:36:00]|13851851802|1       |
    //|[2018-01-29 16:35:00, 2018-01-29 16:36:00]|13851851801|2       |
    //+------------------------------------------+-----------+--------+

    // 1 hours
    //+------------------------------------------+-----------+--------+
    //|datetime                                  |realtelNo  |optTimes|
    //+------------------------------------------+-----------+--------+
    //|[2018-01-29 16:00:00, 2018-01-29 17:00:00]|13851851801|2       |
    //|[2018-01-29 16:00:00, 2018-01-29 17:00:00]|13851851802|1       |
    //+------------------------------------------+-----------+--------+

    //ToFix
    // 1 days
    //+------------------------------------------+-----------+--------+
    //|datetime                                  |realtelNo  |optTimes|
    //+------------------------------------------+-----------+--------+
    //|[2018-01-29 08:00:00, 2018-01-30 08:00:00]|13851851802|1       |
    //|[2018-01-29 08:00:00, 2018-01-30 08:00:00]|13851851801|2       |
    //+------------------------------------------+-----------+--------+
    // 有问题，8点为日的起点，通过window没有找到好办法
    // 把window改成substr(applTime, 1, 10)
    // select substr(applTime, 1, 10) as datetime, realtelNo, count(*) as optTimes
    //   from loan_apply
    // group by substr(applTime, 1, 10), realtelNo
    // 正确的
    //+----------+-----------+--------+
    //|datetime  |realtelNo  |optTimes|
    //+----------+-----------+--------+
    //|2018-01-29|13851851801|2       |
    //|2018-01-29|13851851802|1       |
    //+----------+-----------+--------+

    this.output("update", df2)
  }

  def loanMetric2(stream: DataFrame): Unit = {
    import stream.sparkSession.implicits._

    val df = stream.as[String].map(_.split(","))
      .map(x => LoanApply(x(0), x(1), x(2), x(3))).toDF

    df.createOrReplaceTempView("loan_apply")

    val sql =
      """
        select window(applTime, '1 seconds') as datetime, userId, realtelNo, count(*) as optTimes
          from loan_apply
      group by window(applTime, '1 seconds'), userId, realtelNo
      """
    val df2 = stream.sparkSession.sql(sql)

    //123,lh,13851851801,2018-01-29 16:35:20
    //124,nx,13851851802,2018-01-29 16:35:21
    //125,lh,13851851801,2018-01-29 16:35:20
    //126,lh,13851851803,2018-01-29 16:35:20

    // 1 seconds
    //+------------------------------------------+------+-----------+--------+
    //|datetime                                  |userId|realtelNo  |optTimes|
    //+------------------------------------------+------+-----------+--------+
    //|[2018-01-29 16:35:21, 2018-01-29 16:35:22]|nx    |13851851802|1       |
    //|[2018-01-29 16:35:20, 2018-01-29 16:35:21]|lh    |13851851801|2       |
    //|[2018-01-29 16:35:20, 2018-01-29 16:35:21]|lh    |13851851803|1       |
    //+------------------------------------------+------+-----------+--------+

    this.output("update", df2)
  }

  def output(mode: String, result: DataFrame): Unit = {
    val query = result.writeStream.outputMode(mode).format("console").option("truncate", "false")
      .trigger(Trigger.ProcessingTime(1000)).start()

    query.awaitTermination()
  }

}
