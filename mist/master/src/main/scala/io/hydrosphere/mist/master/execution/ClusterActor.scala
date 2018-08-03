package io.hydrosphere.mist.master.execution

import akka.pattern.pipe
import akka.actor.{Actor, ActorLogging}
import io.hydrosphere.mist.master.execution.workers.{PerJobConnection, WorkerConnection}
import io.hydrosphere.mist.master.models.ContextConfig

import scala.collection.immutable.Queue
import scala.concurrent.{Future, Promise}

sealed trait ReleaseType
final case object Stop extends ReleaseType
final case object Reuse extends ReleaseType

case class PoolSettings(
  min: Int,
  max: Int
)

class ClusterActor(
  id: String,
  name: String,
  idGen: String => String,
  run: String => Future[WorkerConnection]
) extends Actor with ActorLogging {

  import ClusterActor._
  import context._

  override def receive: Receive = init()


  case class State(
    pool: Queue[WorkerConnection],
    inUse: Map[String, WorkerConnection],
    poolSettings: PoolSettings,
    starting: Int
  ) {

    def hasIdle(): Boolean = pool.nonEmpty

    def mayRunNew: Boolean = starting+ inUse.size < poolSettings.max

    def incStarting: State = copy(starting = starting + 1)

    def forgetConn(conn: WorkerConnection): State = copy(inUse = inUse - conn.id)

    def addConn(conn: WorkerConnection, use: Boolean): State =
      if (use) copy(inUse = inUse + (conn.id -> conn)) else copy(pool.enqueue(conn))

    def useIdle(): (WorkerConnection, State) = {
      val (conn, nextPool) = pool.dequeue
      conn -> copy(pool = nextPool, inUse = inUse + (conn.id -> conn))
    }
  }

  object State {
    def empty(settings: PoolSettings): State = State(Queue.empty, Map.empty, settings, 0)
  }

  private def runNew(state: State): State = {
    run(idGen(name)) pipeTo self
    state.incStarting
  }

  private def init(): Receive = {
    case Event.Start(settings, releaseType) =>
      val init = State.empty(settings)
      val next = (0 until settings.min).foldLeft(init)({ case (st, _) => runNew(st)})
      context become process(Queue.empty, releaseType, next)
  }

  private def process(
    requests: Queue[Promise[WorkerConnection]],
    releaseType: ReleaseType,
    state: State
  ): Receive = {
    case Event.Ask(pr) if state.hasIdle() =>
      val (conn, next) = state.useIdle()
      pr.success(conn)
      context become process(requests, releaseType, next)

    case Event.Ask(pr) if state.mayRunNew =>
      val next = runNew(state)
      context become process(requests.enqueue(pr), releaseType, next)

    case Event.Ask(pr) =>
      context become process(requests.enqueue(pr), releaseType, state)

    case conn: WorkerConnection if requests.nonEmpty =>
      val (req, nextReq) = requests.dequeue
      req.success(conn)
      val nextState = state.addConn(conn, true)
      context become process(nextReq, releaseType, nextState)

    case conn: WorkerConnection =>
      val next = state.addConn(conn, false)
      context become process(requests, releaseType, next)

    case Event.Release(conn) if requests.nonEmpty =>
      releaseType match {
        case Reuse =>
        case Stop =>
          conn.shutdown(false)
      }


    case Event.Stop(force) =>
  }

}

object ClusterActor {

  sealed trait Event
  object Event {
    final case class Start(settings: PoolSettings, releaseType: ReleaseType) extends Event
    final case class Stop(force: Boolean) extends Event

    final case class ConnTerminated(id: String) extends Event

    final case class Ask(resolve: Promise[WorkerConnection]) extends Event
    final case class Release(conn: WorkerConnection) extends Event

  }
}
