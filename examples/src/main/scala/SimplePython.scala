import com.provectus.lymph.LymphJob
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext

import py4j.GatewayServer
import sys.process._

import scala.collection.JavaConversions._

object DataWrapper{
  var data: Any = _
  def set(in: Any) = { data = in }
  def set(in: java.util.ArrayList[Int]) = { data = asScalaBuffer(in).toList }
  def get(): Any = data
}

object SparkContextWrapper{
  var m_context : JavaSparkContext = _
  var m_conf : SparkConf = _

  def getSparkConf(): SparkConf = m_conf
  def setSparkConf(conf: SparkConf) = {m_conf = conf}

  def getSparkContext(): JavaSparkContext = m_context
  def setSparkContext(sc: SparkContext) = {m_context = new JavaSparkContext(sc)}
}

object SimplePython extends LymphJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param context    spark context
    * @param parameters user parameters
    * @return result of the job
    */

  def ScalaSparkContextWrapper() = SparkContextWrapper

  def SimpleDataWrapper = DataWrapper

  override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {

    val numbers: List[Int] = parameters("digits").asInstanceOf[List[Int]]

    SimpleDataWrapper.set(numbers)
    ScalaSparkContextWrapper.setSparkConf(context.getConf)
    ScalaSparkContextWrapper.setSparkContext(context)

    val gatewayServer: GatewayServer = new GatewayServer(SimplePython)
    gatewayServer.start()
    val boundPort: Int = gatewayServer.getListeningPort

    if (boundPort == -1) {
      println("GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      println(s"Started PythonGatewayServer on port $boundPort")
    }
    //"export SPARK_HOME=/home/vagrant/spark-1.5.2-bin-hadoop2.6/".!
    //"export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH".!
    var starttime = context.startTime
    println(s"$starttime")

    val cmd = "python /vagrant/examples/src/main/python/example.py"
    val exitCode = cmd.!
    println(exitCode)
    gatewayServer.shutdown()

    println("Exiting due to broken pipe from Python driver")

    Map("result" -> SimpleDataWrapper.get())
  }

}