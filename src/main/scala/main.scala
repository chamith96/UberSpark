import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object main {

  def main(args: Array[String])
  {
    // logs of the app is disabled
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Second").setMaster("local")
    val sc = new SparkContext(conf)
    val dataset = sc.textFile("uber")
    val header = dataset.first()
    val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
    var days =Array("Sun","Mon","Tue","Wed","Thu","Fri","Sat")
    val eliminate = dataset.filter(line => line != header)
    val split = eliminate.map(line => line.split(",")).map { x => (x(0),format.parse(x(1)),x(3)) }
    val combine = split.map(x => (x._1+" "+days(x._2.getDay),x._3.toInt))
    val arrange = combine.reduceByKey(_+_).map(item => item.swap).sortByKey(false).collect.foreach(println)
    println(arrange)
  }
}
