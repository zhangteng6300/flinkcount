import org.apache.flink.streaming.api.scala._

/**
  * Created by 86977 on 2020/5/29.
  */
object csv_read {
  def main(args: Array[String]): Unit = {
    val  env=StreamExecutionEnvironment.getExecutionEnvironment
    val  DString:DataStream[String] =env.readTextFile("D:\\project\\flinkcount\\src\\main\\resources\\data02")
   // val result:DataStream[String,String,String]=DString.flatMap(_.split(","))

    DString.print()

    env.execute("Socket stream word count")
  }
}
