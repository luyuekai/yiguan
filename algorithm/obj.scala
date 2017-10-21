
case class Log(timestamp: Int, opId: Int)


case class SearchParams(beginDate:Int,endDate:Int,opParams:Array[OpParams],timeWin:Int){
  val opArr = opParams.map((x)=>{
    x.opId
  })

  val opSet = opArr.toSet
}

case class OpParams(opId:Int,or:Array[OpCondition]=Array(),and:Array[OpCondition]=Array())

case class OpCondition(t:Int,key:String="",v:String="",rel:String="")

//OpCondition 说明
//t: 类型 0-字符类型 1-数字类型
//key: 字符型的关键字
//num: 数字类型的关键字
//rel: 数字类型的关系处理
