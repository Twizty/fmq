import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.{Executors, TimeUnit}

import cats.effect._
import cats.effect.concurrent._
import cats.effect.implicits._
import cats.syntax.all._
import fs2.{Chunk, Pipe, Stream}
import fs2.{concurrent => async}
import fs2.Stream.eval
import fs2.io.tcp.{Socket, server}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import fs2.concurrent.{Queue, Signal}


object Example extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {

    implicit val group = AsynchronousChannelGroup.withThreadPool(Executors.newSingleThreadExecutor())
    implicit val ec = ExecutionContext.global
    implicit val contextShift: ContextShift[IO] = IO.contextShift(ec)
    val state = new State()
    val sink = new EventSink[IO]()

    Stream.eval(async.Topic[IO, Subscription[IO]](Subscription[IO](null, Array(), "", null, 0))).flatMap { t =>
      Stream.eval(async.Topic[IO, Event](DummyEvent)).flatMap { t2 =>
        Stream.eval(MVar.of[IO, Map[String, Set[String]]](Map.empty[String, Set[String]])).flatMap { mvar1 =>
          Stream.eval(MVar.of[IO, Map[String, Queue[IO, String]]](Map.empty[String, Queue[IO, String]])).flatMap { mvar2 =>
            val s = new Server[IO](8765, 100, 1000, mvar1, mvar2)
            s.startServer(t, t2)
              .concurrently(t.subscribe(100)
                .through(sink.sinkSubscriptions(t2))
                .parJoin(100))
              .drain
          }
        }
      }
    }.compile.drain.as(ExitCode.Success)
  }
}

case class Subscription[F[_]](socket: Socket[F],
                              channels: Array[String],
                              addr: String,
                              signal: async.SignallingRef[F, Boolean],
                              time: Long)

sealed trait Event {
  def time: Long
}
case object DummyEvent extends Event {
  def time = 0l
}
case class MessageEvent(responders: Set[String], payload: String, time: Long) extends Event
case class UnsubscribeEvent(host: String, channels: Array[String], time: Long) extends Event
case class ExitEvent(host: String, time: Long) extends Event

class Server[F[_]](addr: Int,
                   maxConcurrent: Int,
                   readChunkSize: Int,
                   state: MVar[F, Map[String, Set[String]]],
                   qState: MVar[F, Map[String, Queue[F, String]]])(
  implicit AG: AsynchronousChannelGroup,
  F: Effect[F],
  C: Concurrent[F],
  T: Timer[F],
  CF: ConcurrentEffect[F],
  ec: ExecutionContext
) {
  val queueState = new QueueState[F](qState)
  val endingsOfTheLine = Array(Array[Byte](10, 13))
  val functionalState = new FunctionalState[F](state)

  def startServer(topic: async.Topic[F, Subscription[F]],
                  eventTopic: async.Topic[F, Event]): Stream[F, Unit] = {
    val s = server(new InetSocketAddress(addr))
    s.map[Stream[F, Unit]] { r =>
      eval(r.use { socket =>
        socket.remoteAddress.flatMap { remoteAddr =>
          async.SignallingRef(false).flatMap { initial =>
            socket.reads(readChunkSize, None)
              .through(parseBody)
              .attempt
              .evalMap {
                case Left(t) =>
                  println(t)
                  F.pure(())
                case Right(body) if body == List(10, 13) =>
                  F.pure(())
                case Right(body) if endingsOfTheLine.find(body.startsWith(_)).isDefined =>
                  CommandParser.parse(body.reverse.map(_.toChar).mkString) match {
                    case Some(Publish(channel, data)) => publish(socket, eventTopic, channel, data)
                    case Some(Subscribe(channels)) => subscribe(initial, socket, topic, channels)
                    case Some(Unsubscribe(channels)) => unsubscribe(socket, eventTopic, channels)
                    case Some(Push(queue, data)) => push(queue, data)
                    case Some(Pull(queue, timeout)) => pull(socket, queue, timeout)
                    case Some(Exit) => exit(socket, eventTopic)
                    case None => respond(socket, Some("invalid command\n".getBytes))
                  }
                case Right(_) => F.pure(())
              }.onFinalize(
                eventTopic.publish1(ExitEvent(remoteAddr.toString, System.currentTimeMillis())).flatMap { _ =>
                  functionalState deleteHost remoteAddr.toString
                }
              ).compile.drain
          }
        }
      })
    }.parJoin(maxConcurrent)
  }

  def respond(socket: Socket[F], resp: Option[Array[Byte]]): F[Unit] = {
    resp match {
      case Some(d) => socket.write(Chunk.bytes(d))
      case None => F.pure()
    }
  }

  def push(queue: String, data: String): F[Unit] = {
    F.flatMap(queueState.get(queue)) { q =>
      q.enqueue1(data)
    }
  }

  def pull(socket: Socket[F], queue: String, timeout: Option[FiniteDuration]): F[Unit] = {
    F.flatMap(queueState.get(queue)) { q =>
      timeout match {
        case Some(d) =>
          F.flatMap(Concurrent.timeout(q.dequeue1, d).attempt) {
            case Right(res) => socket.write(Chunk.bytes(("result: " + res).getBytes()))
            case Left(_) => socket.write(Chunk.bytes("error: timeout".getBytes()))
          }
        case None =>
          F.flatMap(q.dequeue1) { res =>
            socket.write(Chunk.bytes(("result: " + res).getBytes()))
          }
      }
    }
  }

  def publish(socket: Socket[F], topic: async.Topic[F, Event], channel: String, data: String): F[Unit] ={
    F.flatMap(functionalState.get(channel)) {
      case Some(set) if set.nonEmpty =>
        topic.publish1(MessageEvent(set, data, System.currentTimeMillis()))
      case _ =>
        socket.write(Chunk.bytes("no subscribers for given channel\n".getBytes))
    }
  }

  def subscribe(alreadySubscribed: async.SignallingRef[F, Boolean], socket: Socket[F], topic: async.Topic[F, Subscription[F]], channels: Array[String]): F[Unit] = {
    F.flatMap(socket.remoteAddress) { a =>
      F.flatMap(functionalState.add(a.toString, channels)) { _ =>
        F.flatMap(alreadySubscribed.get) { subscribed =>
          if (subscribed) {
            F.pure(())
          } else {
            F.flatMap(alreadySubscribed.set(true)) { _ =>
              F.flatMap(async.SignallingRef(false)) { sig =>
                topic.publish1(
                  Subscription(socket, channels, a.toString, sig, System.currentTimeMillis())
                )
              }
            }
          }
        }
      }
    }
  }

  def unsubscribe(socket: Socket[F], topic: async.Topic[F, Event], channels: Array[String]): F[Unit] = {
    F.flatMap(socket.remoteAddress) { a =>
      F.flatMap(functionalState.deleteExchanges(a.toString, channels)) { _ =>
        topic.publish1(UnsubscribeEvent(a.toString, channels, System.currentTimeMillis()))
      }
    }
  }

  def exit(socket: Socket[F], topic: async.Topic[F, Event]): F[Unit] = {
    F.flatMap(socket.remoteAddress) { a =>
      F.flatMap(functionalState.deleteHost(a.toString)) { _ =>
        F.flatMap(socket.write(Chunk.bytes("bye\n".getBytes))) { _ =>
          F.flatMap(topic.publish1(ExitEvent(a.toString, System.currentTimeMillis()))) { _ =>
            socket.close
          }
        }
      }
    }
  }

  def generateId = java.util.UUID.randomUUID.toString

  def parseBody[F[_], O]: Pipe[F, O, List[O]] =
    _.scan(List[O]()) { (acc, b) =>
      if (endingsOfTheLine.find(e => acc.startsWith(e)).isDefined) {
        List[O](b)
      } else {
        b :: acc
      }
    }
}