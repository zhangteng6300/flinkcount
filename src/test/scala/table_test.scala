import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * Created by 86977 on 2020/5/29.
  */
case class test_promotion_keyword(account: String,create_time: String,promotion_plan: String)

object table_test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[String] = env.readTextFile("D:\\project\\flinkcount\\src\\main\\resources\\data02")
    val dataStream: DataStream[test_promotion_keyword] = inputStream.map(data => {
      val dataArray = data.split(",")
      test_promotion_keyword(dataArray(0).replaceAll("""\"""",""), dataArray(1).replaceAll("""\"""",""), dataArray(2).replaceAll("""\"""","").toString)
    })
   // dataStream.print()

    //分流

    val splitStream:SplitStream[test_promotion_keyword]=dataStream.split(data => {
      if(data.promotion_plan.length() > 5)
        Seq("right")
      else
        Seq("ERROR")

    })
val rightStream:DataStream[test_promotion_keyword]=splitStream.select("right")
    val errorStream:DataStream[test_promotion_keyword]=splitStream.select("ERROR")

   rightStream.print("right")
    rightStream.print("ERROR")



    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val dataTable: Table = tableEnv.fromDataStream(dataStream)
    val resultTable: Table = dataTable
      .select("account,create_time,promotion_plan")
   // val resultSream: DataStream[(String, String, String)] = resultTable.

    env.execute("table example job")
  }
}
