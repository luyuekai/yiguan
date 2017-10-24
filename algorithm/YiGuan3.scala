
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable.ArrayBuffer

object YiGuan3 {
  200000000L
  //  val beginDate = 0
  val startDate = 1493568000000L //5月1日

  val beginDate = ((1496246400000L - 1493568000000L) / 10).toInt
  val endDate = ((1498838400000L - 1493568000000L) / 10).toInt
  //时间窗口
  val timeWin = 3600 * 24 * 3 * 100

  val ops = Array(OpParams(5), OpParams(9), OpParams(10))


  val searchParams = SearchParams(beginDate, endDate, ops, timeWin)

  //  读程序
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("yiguan")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //使用Kryo序列化库,速度并未有提升
    conf.registerKryoClasses(Array(classOf[Log]))


    val sc = new SparkContext(conf)

    val rs = new Array[Accumulator[Int]](searchParams.opParams.length)
    // 初始化累加器，用于统计每一步操作ID 的转化数
    for (i <- 0 until rs.length) {
      rs(i) = sc.accumulator(0)
    }
    
    // 用于计时
    val start = System.nanoTime() 
    
    val data = sc.textFile(args(1))
      .map((line) => {    // 一行一行（一个用户一个用户）的处理
        val lines = line.split('|')
        val logs = ArrayBuffer[Log]()
        for (i <- 0 until lines.length by 3) {
          val opId = lines(i).toInt
          val timestamp = lines(i + 1).toInt
          val params = lines(i + 2)
          if (filter(opId, timestamp, params)) {
            logs += Log(timestamp, opId)
          }
        }
        val longestPath = getLongestPath2(logs)  //得到该用户的最大转化深度

        // 累加器统计结果
        for (i <- 0 until longestPath) {
          rs(i) += 1
        }
        (longestPath, 1)
      })
      .count()  //在正式跑算法前，提前触发spark的action算子，将数据先加载至内存中，减少数据加载耗时。（平均会提升1.5秒到2.5秒）

    val end = System.nanoTime()
    println(((end - start) / 1000 / 1000).toString + "ms")


   // 输出每一步转化的统计值
    for (i <- 0 until rs.length) {
      println(rs(i).value)
    }

  }

  def getLongestPath2(data: ArrayBuffer[Log]): Int = {
    val g = data
    val timestamp = new Array[Int](16)  // 用于记录能构成当前转化序列的最新的起始ID时间戳（具体解释，见MarkDown算法思想）。 操作ID经过预处理范围是（1到15）与timestamp数组的下表相对应
    val preOp = new Array[Int](16)      // 用于记录操作ID之间的顺序关系，每一个数组值的地方是上一个操作ID
    for (i <- 1 until searchParams.opArr.length) {
      preOp(searchParams.opArr(i)) = searchParams.opArr(i - 1)
    }

    g.foreach((x) => {

      if (x.opId == searchParams.opArr(0)) {  //如果操作ID 是第一个，则更新对应的时间戳
        timestamp(x.opId) = x.timestamp
      } else if (timestamp(preOp(x.opId)) > 0 && x.timestamp - timestamp(preOp(x.opId)) <= searchParams.timeWin) {  //如果操作ID不是第一个，判断前一个时间戳是否有值。若有值，判断当前操作ID的时间戳与前一个存储的起始ID时间戳是否在一个时间窗口内，如果在，则更新对应位置的timestamp
        timestamp(x.opId) = timestamp(preOp(x.opId))

        //如果完成所有转化，则直接返回最大长度
        if (x.opId == searchParams.opArr(searchParams.opArr.length - 1)) {
          return searchParams.opArr.length
        }
      }
    })

    //没有完成最终转化，返回最深的长度
    for (i <- searchParams.opArr.length - 1 to 0 by -1) {
      if (timestamp(searchParams.opArr(i)) > 0) {
        return i + 1
      }
    }

    return 0
  }

  def filter(opId: Int, timestamp: Int, opAttrs: String): Boolean = {
    //判断时间
    if (timestamp < searchParams.beginDate || timestamp > searchParams.endDate) {
      return false
    }

    //判断参数
    for (i <- 0 until searchParams.opParams.length) {
      if (searchParams.opParams(i).opId == opId) {
        if (searchParams.opParams(i).or.length != 0) {
          implicit val format = DefaultFormats
          val jsonObj = parse(opAttrs).extract[Map[String, String]]
          val orArr = searchParams.opParams(i).or

          orArr.foreach((condition) => {
            val v = jsonObj.getOrElse(condition.key, "")
            if (v != "" && isOk(v, condition)) {
              return true
            }
          })

          return false
        } else if (searchParams.opParams(i).and.length != 0) {
          implicit val format = DefaultFormats
          val jsonObj = parse(opAttrs).extract[Map[String, String]]
          val andArr = searchParams.opParams(i).and

          var ok = true
          andArr.foreach((condition) => {
            val v = jsonObj.getOrElse(condition.key, "")
            if (v == "" || isOk(v, condition) == false) {
              ok = false
            }
          })

          return ok
        } else {
          //            不存在过滤条件
          return true
        }
      }
    }

    return false
  }

  def isOk(params: String, condition: OpCondition): Boolean = {
    //0表示字符串
    if (condition.t == 0) {
      if (condition.v == params) {
        return true
      }
    } else if (condition.t == 1) {
      val p = params.toDouble
      val num = condition.v.toDouble
      condition.rel match {
        case "eq" => if (p == num) {
          return true
        }
        case "lg" => if (p > num) {
          return true
        }
        case "el" => if (p >= num) {
          return true
        }
        case "sm" => if (p < num) {
          return true
        }
        case "es" => if (p <= num) {
          return true
        }
      }
    }

    false
  }

}





