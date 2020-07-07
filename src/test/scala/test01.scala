import java.time.LocalDateTime

/**
  * Created by 86977 on 2020/5/28.
  */
object test01 {
  def main(args: Array[String]): Unit = {
    val curTime=LocalDateTime.now().toString.replace("T", " ")
    print(curTime)
  }
}
