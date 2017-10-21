
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object YiGuan2 {
  //  写程序
  //  将数据处理按照user进行分组的数据
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("yiguan")


    val sc = new SparkContext(conf)

    var data =
      sc.textFile(args(0))
        .map(_.split("\t+"))
        .filter(_.length > 5)
        .map(items => {
          val userId = items(0)
     //将时间减去20170501的UTC时间，再除以10，变做Int型存储。
          val timestamp = ((items(1).toLong - 1493568000000L)/10).toInt
          val opId = items(2).substring(2).toInt
          val params = items(4)
          (userId, timestamp, opId, params)
        })

    //   预处理数据，将结果存储至文件，便于后续算法使用。
    data
      .groupBy(_._1)
      .map((x) => {
        var rs = ""
        x._2.toArray.sortBy(_._2)
          .foreach((item) => {
            rs += item._3 + "|" + item._2 + "|" + item._4 + "|"
          })
        rs
      })
      .saveAsTextFile(args(1))
  }
}



