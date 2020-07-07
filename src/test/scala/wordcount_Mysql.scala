import java.sql.{Connection, PreparedStatement}

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

/**
  * Created by 86977 on 2020/5/29.
  */
object wordcount_Mysql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val DString: DataStream[String] = env.readTextFile("D:\\project\\flinkcount\\src\\main\\resources\\word")
    val result: DataStream[(String, Integer)] = DString
      .flatMap(_.split(" ")).map((_, 1))
      .keyBy(0).sum(1).map(x => {
      (x._1, (new Integer(x._2)))
    })

    val sink: Conn2Mysql = new Conn2Mysql("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false", "root", "123456")
    //result.print()
    result.addSink(sink)
    env.execute("Socket stream word count")
  }

}

