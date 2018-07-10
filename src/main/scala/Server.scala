import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.{Executors, TimeUnit}

import cats.effect.{Effect, IO}
import fs2.{Chunk, Pipe, Stream, async}
import fs2.Stream.eval
import fs2.io.tcp.{Socket, server}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import fs2.async.mutable.Signal


object Example {
  def main(args: Array[String]): Unit = {
    implicit val group = AsynchronousChannelGroup.withThreadPool(Executors.newSingleThreadExecutor())
    implicit val ec = ExecutionContext.global
    val state = new State()
    val s = new Server[IO](8765, 100, 1000, FiniteDuration(200, TimeUnit.SECONDS), state)
    val sink = new EventSink[IO](state)
    Stream.eval(async.topic[IO, Subscription[IO]](Subscription[IO](null, Array(), "", 0))).flatMap { t =>
      Stream.eval(async.topic[IO, Event](DummyEvent)).flatMap { t2 =>
        s.startServer(t, t2)
         .concurrently(t.subscribe(100)
         .through(sink.sinkSubscriptions(t2))
         .join(100))
         .drain
      }
    }.compile.drain.unsafeRunSync()

    //    val sch = Scheduler[IO](4)
    //
    //    sch.flatMap { sc =>
    //      println(System.currentTimeMillis)
    //      sc.delay(Stream.eval(IO {
    //        Left("exception"): Either[String, Long]
    //      }), 2.second)
    //    }.merge(Stream.eval(IO {
    //      Thread.sleep(1500); Right(System.currentTimeMillis): Either[String, Long]
    //    })).head.map {
    //      case Left(ex) => println("boom!! " + ex)
    //      case Right(v) => println(v)
    //    }.compile.toList.unsafeRunSync()
  }
}

case class Subscription[F[_]](socket: Socket[F], channels: Array[String], addr: String, time: Long)

sealed trait Event {
  def time: Long
}
case object DummyEvent extends Event {
  def time = 0l
}
case class MessageEvent(channel: String, payload: String, time: Long) extends Event
case class UnsubscribeEvent(host: String, channels: Array[String], time: Long) extends Event
case class ExitEvent(host: String, time: Long) extends Event

class Server[F[_]](addr: Int,
                   maxConcurrent: Int,
                   readChunkSize: Int,
                   readTimeouts: FiniteDuration,
                   state: State)(
  implicit AG: AsynchronousChannelGroup,
  F: Effect[F],
  ec: ExecutionContext
) {
  var endingsOfTheLine = Array(Array[Byte](10, 13))

  def startServer(topic: async.mutable.Topic[F, Subscription[F]], eventTopic: async.mutable.Topic[F, Event]): Stream[F, Unit] = {
    val s = server(new InetSocketAddress(addr))
    s.map { x =>
      x.flatMap { socket =>
        eval(fs2.async.signalOf(false)).flatMap { initial =>
          socket.reads(readChunkSize, None)
            .through(parseBody)
            .attempt
            .evalMap {
              case Left(t) =>
                println(t)
                F.pure(())
              case Right(body) if endingsOfTheLine.find(body.startsWith(_)).isDefined =>
                CommandParser.parse(body.reverse.map(_.toChar).mkString) match {
                  case Some(Push(channel, data)) => publish(socket, eventTopic, channel, data)
                  case Some(Subscribe(channels)) => subscribe(initial, socket, topic, channels)
                  case Some(Unsubscribe(channels)) => unsubscribe(socket, eventTopic, channels)
                  case Some(Exit) => exit(socket, eventTopic)
                  case None => respond(socket, Some("invalid command\n".getBytes))
                }
              case Right(_) => F.pure(())
            }.drain
        }
      }
    }.join(maxConcurrent)
  }

  def respond(socket: Socket[F], resp: Option[Array[Byte]]): F[Unit] = {
    resp match {
      case Some(d) => socket.write(Chunk.bytes(d))
      case None => F.pure()
    }
  }

  def publish(socket: Socket[F], topic: async.mutable.Topic[F, Event], channel: String, data: String): F[Unit] ={
    if (state.exchangeExists(channel)) {
      topic.publish1(MessageEvent(channel, data, System.currentTimeMillis()))
    } else {
      socket.write(Chunk.bytes("no subscribers for given channel\n".getBytes))
    }
  }

  def subscribe(alreadySubscribed: async.mutable.Signal[F, Boolean], socket: Socket[F], topic: async.mutable.Topic[F, Subscription[F]], channels: Array[String]): F[Unit] = {
    F.flatMap(socket.remoteAddress) { a =>
      state.add(a.toString, channels)
      F.flatMap(alreadySubscribed.get) { subscribed =>
        if (subscribed) {
          F.pure(())
        } else {
          F.flatMap(alreadySubscribed.set(true)) { _ =>
            topic.publish1(Subscription(socket, channels, a.toString, System.currentTimeMillis()))
          }
        }
      }
    }
  }

  def unsubscribe(socket: Socket[F], topic: async.mutable.Topic[F, Event], channels: Array[String]): F[Unit] = {
    F.flatMap(socket.remoteAddress) { a =>
      state.unsubscribeExchanges(a.toString, channels)
      topic.publish1(UnsubscribeEvent(a.toString, channels, System.currentTimeMillis()))
    }
  }

  def exit(socket: Socket[F], topic: async.mutable.Topic[F, Event]): F[Unit] = {
    F.flatMap(socket.remoteAddress) { a =>
      state.unsubscribeHost(a.toString)
      topic.publish1(ExitEvent(a.toString, System.currentTimeMillis()))
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