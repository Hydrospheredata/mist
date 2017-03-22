package io.hydrosphere.mist

//import akka.actor.{ActorSystem, Props}
//import akka.testkit.TestKit
//import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
//import org.scalatest.concurrent.{Eventually, ScalaFutures}
//import spray.json._
//import DefaultJsonProtocol._
//import io.hydrosphere.mist.jobs.MistJobConfiguration
//import io.hydrosphere.mist.master.async.mqtt.{MQTTSubscribe, MqttSubscriber$}
//import io.hydrosphere.mist.master.cluster.ClusterManager
//import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
//import io.hydrosphere.mist.worker.{ContextNode, JobRunnerNode}
//import org.eclipse.paho.client.mqttv3.{IMqttDeliveryToken, MqttCallback, MqttClient, MqttMessage}
//import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
//
//import scala.concurrent.duration._
//
//class InfinityJobTestActor extends WordSpecLike with Eventually with BeforeAndAfterAll with ScalaFutures with Matchers with JobConfigurationJsonSerialization with DefaultJsonProtocol {
//
//  val systemM = ActorSystem("mist", MistConfig.Akka.Main.settings)
//  val systemW = ActorSystem("mist", MistConfig.Akka.Worker.settings)
//  val systemS = ActorSystem("mist", MistConfig.Akka.Worker.settings)
//
//  val mqttActor = systemW.actorOf(Props(classOf[MqttSubscriber]))
//  mqttActor ! MQTTSubscribe
//  InfinityJobTestMqttActor.subscribe(systemW)
//
//  override def beforeAll() = {
//    systemM.actorOf(Props[ClusterManager], name = Constants.Actors.clusterManagerName)
//    Thread.sleep(5000)
//  }
//
//  override def afterAll() = {
//    InfinityJobTestMqttActor.disconnect()
//
//    TestKit.shutdownActorSystem(systemW)
//    TestKit.shutdownActorSystem(systemS)
//    TestKit.shutdownActorSystem(systemM)
//    Thread.sleep(5000)
//  }
//
//  "Streaming" must {
//    "Start and success" in {
//
//      systemW.actorOf(ContextNode.props("streaming"), name = "streaming")
//      Thread.sleep(5000)
//
//      MqttSuccessObj.success = false
//      systemS.actorOf(JobRunnerNode.props(MistJobConfiguration(TestConfig.examplesPath, "SimpleSparkStreaming$", "streaming", Map().empty, Some("123456789"))))
//
//      eventually(timeout(30 seconds), interval(1 second)) {
//        assert(MqttSuccessObj.success)
//      }
//    }
//  }
//}
//
//object InfinityJobTestMqttActor {
//
//  val persistence = new MemoryPersistence
//  val mqttClient = new MqttClient(s"tcp://${MistConfig.Mqtt.host}:${MistConfig.Mqtt.port}", MqttClient.generateClientId, persistence)
//
//  def subscribe(actorSystem: ActorSystem): Unit = {
//    mqttClient.connect()
//    mqttClient.subscribe(MistConfig.Mqtt.subscribeTopic)
//
//    val callback = new MqttCallback {
//      override def messageArrived(topic: String, message: MqttMessage): Unit = {
//        println("Receiving Data Test, Topic : %s, Message : %s".format(topic, message))
//        val stringMessage = message.toString
//
//        val response = stringMessage.parseJson.convertTo[Map[String, JsValue]]
//        if (response.contains("time") && response.contains("length") && response.contains("collection")) {
//          MqttSuccessObj.success = true
//        }
//      }
//
//      override def connectionLost(cause: Throwable): Unit = {
//        println(cause)
//      }
//
//      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
//      }
//    }
//    mqttClient.setCallback(callback)
//  }
//
//  def disconnect(): Unit = {
//    mqttClient.disconnect()
//  }
//}