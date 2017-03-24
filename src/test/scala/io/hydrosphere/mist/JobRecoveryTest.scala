package io.hydrosphere.mist

//import java.io.{File, FileInputStream, FileOutputStream}
//
//import akka.actor.{ActorSystem, Props}
//import akka.testkit.{ImplicitSender, TestKit}
//import io.hydrosphere.mist.jobs.store.JobRepository
//import io.hydrosphere.mist.jobs.{FullJobConfiguration, InMapDbJobConfigurationRepository, InMemoryJobConfigurationRepository}
//import io.hydrosphere.mist.master._
//import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
//import org.mapdb.{DBMaker, Serializer}
//import org.scalatest.concurrent.{Eventually, ScalaFutures}
//import org.scalatest.time.{Second, Seconds, Span}
//import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
//import spray.json.{DefaultJsonProtocol, _}
//
//import scala.concurrent.duration._
//
//class JobRecoveryTest(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers
//  with BeforeAndAfterAll with ScalaFutures with JobConfigurationJsonSerialization with DefaultJsonProtocol with Eventually {
//
//  def this() = this(ActorSystem("JobRecoveryTestActorSystem"))
//
//  override def afterAll(): Unit = {
//    Thread.sleep(5000)
//    TestKit.shutdownActorSystem(system)
//    TestKit.shutdownActorSystem(_system)
//    Thread.sleep(5000)
//  }
//
//  override def beforeAll(): Unit = {
//    Thread.sleep(5000)
//    val db = DBMaker
//      .fileDB(MistConfig.Recovery.recoveryDbFileName + "b")
//      .make
//
//    // Map
//    val map = db
//      .hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY)
//      .createOrOpen
//
//    val stringMessage = TestConfig.requestJar
//    val json = stringMessage.parseJson
//    val jobCreatingRequest = json.convertTo[FullJobConfiguration]
//    val w_job = jobCreatingRequest.toJson.compactPrint.getBytes
//    map.clear()
//    for (i <- 1 to 3) {
//      map.put("3e72eaa8-682a-45aa-b0a5-655ae8854c" + i.toString, w_job)
//    }
//
//    map.close()
//    db.close()
//
//    val src = new File(MistConfig.Recovery.recoveryDbFileName + "b")
//    val dest = new File(MistConfig.Recovery.recoveryDbFileName)
//    new FileOutputStream(dest) getChannel() transferFrom(
//      new FileInputStream(src) getChannel, 0, Long.MaxValue)
//  }
//
//  "Recovery 3 jobs" must {
//    "All recovered ok" in {
//      var configurationRepository: JobRepository = InMemoryJobConfigurationRepository
//
//      if (MistConfig.Recovery.recoveryOn) {
//        configurationRepository = MistConfig.Recovery.recoveryTypeDb match {
//          case "MapDb" => InMapDbJobConfigurationRepository
//          case _ => InMemoryJobConfigurationRepository
//        }
//      }
//
//      lazy val recoveryActor = system.actorOf(Props(classOf[JobRecovery], configurationRepository))
//
//      recoveryActor ! StartRecovery
//
//      assert(configurationRepository.size == 3)
//
//      eventually (timeout(10 seconds), interval(1 second)) {
//        recoveryActor ! JobStarted
//        recoveryActor ! JobCompleted
//        assert(TryRecoveryNext._collection.isEmpty && configurationRepository.size == 0)
//      }
//    }
//  }
//
//  override implicit def patienceConfig: PatienceConfig = PatienceConfig(Span(60, Seconds), Span(1, Second))
//}