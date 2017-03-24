package io.hydrosphere.mist

//import akka.actor.{Actor, ActorRef, ActorSystem, Props}
//import akka.pattern.ask
//import akka.testkit.{ImplicitSender, TestKit}
//import io.hydrosphere.mist.master.async.mqtt._
//import io.hydrosphere.mist.master.async.mqtt.MqttActorWrapper._
//import io.hydrosphere.mist.SubsActor.Report
//import org.scalatest.concurrent.ScalaFutures
//import org.scalatest.time.{Second, Seconds, Span}
//import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

//class MqttTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers
//  with BeforeAndAfterAll with ScalaFutures {
//  import system.dispatcher
//
//  def this() = this(ActorSystem("MqttTestActorSystem"))
//
//  override def afterAll() = {
//    TestKit.shutdownActorSystem(system)
//    TestKit.shutdownActorSystem(_system)
//  }
//
//  "MqttPubSub" must {
//    "Mqtt ok" in {
//      val count = 10
//
//      val subs = system.actorOf(Props(classOf[SubsActor], testActor, count))
//      subs ! SubscribeTest
//
//      within(30.seconds) {
//        expectMsgType[SubscribeAck]
//      }
//
//      subs ! PublishTest
//
//      var receivedCount = 0
//
//      implicit val askTimeout = akka.util.Timeout(1, SECONDS)
//
//      for (delay <- 1 to 50 if receivedCount < count) {
//        receivedCount = akka.pattern.after(1.seconds, system.scheduler)(subs ? Report).mapTo[Int].futureValue
//        println(s"$delay: Pub $count Rec $receivedCount ~ ${receivedCount * 100.0 / count}%")
//      }
//      assert(receivedCount == count)
//    }
//  }
//
//  override implicit def patienceConfig: PatienceConfig = PatienceConfig(Span(60, Seconds), Span(1, Second))
//}
//
//private case object SubscribeTest
//private case object PublishTest
//
//private object SubsActor {
//  case object Report
//}
//
//private class SubsActor(reporTo: ActorRef, count: Int) extends Actor with MQTTPubSubActor {
//  import SubsActor._
//  def receive = {
//    case SubscribeTest => pubsub ! Subscribe(self)
//    case msg @ SubscribeAck(Subscribe(`self`)) =>
//      context become ready
//      reporTo ! msg
//  }
//
//  private[this] var counter = 0
//  def ready: Receive = {
//    case msg: Message => if( "testmsg" == new String(msg.payload, "utf-8") ) counter += 1
//    case Report       => sender() ! counter
//    case PublishTest  =>
//      var i = 0
//      while (i < count) {
//        val payload = "testmsg".getBytes("utf-8")
//        pubsub ! new Publish(payload)
//        i += 1
//      }
//  }
//}