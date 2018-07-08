import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.{Executors, TimeUnit, TimeoutException}

import cats.effect.{Effect, IO}
import fs2.{Chunk, Pipe, Stream, async}
import fs2.Stream.eval
import fs2.io.tcp.{Socket, server}

import scala.collection.concurrent
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.collection._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}


object Example {
  def main(args: Array[String]): Unit = {
    implicit val group = AsynchronousChannelGroup.withThreadPool(Executors.newSingleThreadExecutor())
    implicit val ec = ExecutionContext.global
    val s = new Server[IO](8765, 100, 1000, FiniteDuration(200, TimeUnit.SECONDS))
    Stream.eval(async.topic[IO, Subscription[IO]](Subscription[IO](null, Array(), "", 0))).flatMap { t =>
      Stream.eval(async.topic[IO, Event](DummyEvent)).flatMap { t2 =>
        s.startServer(t, t2) {
          case Request(body) => Some(body)
          case Error(t) => {
            println(t)
            None
          }
        }.concurrently(t.subscribe(100).through(s.sinkSubscriptions(t2)).join(3)).drain
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

sealed trait Action
case class Request(body: Array[Byte]) extends Action
case class Error(t: Throwable) extends Action

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

sealed trait Command
case class Push(channel: String, data: String) extends Command
case class Subscribe(channels: Array[String]) extends Command
case class Unsubscribe(channels: Array[String]) extends Command
case object Exit extends Command

class Server[F[_]](addr: Int,
                   maxConcurrent: Int,
                   readChunkSize: Int,
                   readTimeouts: FiniteDuration)(
  implicit AG: AsynchronousChannelGroup,
  F: Effect[F],
  ec: ExecutionContext
) {
  type Handler = Action => Option[Array[Byte]]
  var endingsOfTheLine = Array(Array[Byte](10, 13))
  val state = new State()

  def sinkSubscriptions(topic: async.mutable.Topic[F, Event]): Pipe[F, Subscription[F], Stream[F, Either[Throwable, Unit]]] = {
    s =>
      s.filter(_.socket != null).map(s => {
        topic.subscribe(100)
          .filter(e => { println(e); e != DummyEvent })
          .filter(_.time >= s.time)
          .filter {
            case MessageEvent(channel, _, _) => state.hostState(channel, s.addr).isDefined
            case UnsubscribeEvent(host, _, _) => host == s.addr
            case ExitEvent(host, _) => host == s.addr
          }
          .evalMap[Unit](handleEvent(_, s))
          .attempt
      })
  }

  def handleEvent(e: Event, s: Subscription[F]): F[Unit] = {
    e match {
      case MessageEvent(channel, payload, _) =>
        state.hostState(channel, s.addr) match {
          case Some(Subscribed | Unsubscribed) => s.socket.write(Chunk.bytes(payload.getBytes))
          case None => F.pure(())
        }
      case UnsubscribeEvent(_, channels, _) =>
        state.deleteExchanges(s.addr, channels)
        s.socket.write(Chunk.bytes(s"unsubscribed from ${channels.mkString(", ")}".getBytes))
      case ExitEvent(_, _) =>
        state.deleteHost(s.addr)
        F.flatMap(s.socket.write(Chunk.bytes("bye\n".getBytes)))(_ => s.socket.close)
    }
  }

  def startServer(topic: async.mutable.Topic[F, Subscription[F]], eventTopic: async.mutable.Topic[F, Event])(handler: Handler): Stream[F, Unit] = {
    val s = server(new InetSocketAddress(addr))
    s.map { x =>
      x.flatMap { socket =>
        eval(fs2.async.signalOf(false)).flatMap { initial =>
          readWithTimeout[F](socket, readTimeouts, initial.get, readChunkSize)
            .through(parseBody)
            .attempt
            .evalMap {
              case Left(t) =>
                respond(socket, handler(Error(t)))
              case Right(body) if endingsOfTheLine.find(body.startsWith(_)).isDefined =>
                parseCommand(body.reverse.map(_.toChar).mkString) match {
                  case Some(Push(channel, data)) => publish(socket, eventTopic, channel, data)
                  case Some(Subscribe(channels)) => subscribe(socket, topic, channels)
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

  def subscribe(socket: Socket[F], topic: async.mutable.Topic[F, Subscription[F]], channels: Array[String]): F[Unit] = {
    F.flatMap(socket.remoteAddress) { a =>
      state.add(a.toString, channels)
      topic.publish1(Subscription(socket, channels, a.toString, System.currentTimeMillis()))
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

  def parseCommand(q: String): Option[Command] = {
    q.split(' ') match {
      case Array(f, s, t) if f == "push" => Some(Push(s, t.trim)) // TODO: fix parsing
      case Array(f, s) if f == "subscribe" => Some(Subscribe(s.trim.split(',')))
      case Array(f, s) if f == "unsubscribe" => Some(Unsubscribe(s.trim.split(',')))
      case a if a.length >= 1 && a(0).startsWith("exit") => Some(Exit)
      case _ =>
        None
    }
  }

  def parseBody[F[_], O]: Pipe[F, O, List[O]] =
    in => {
      in.scan(List[O]()) { (acc, b) => {
        if (endingsOfTheLine.find(e => acc.startsWith(e)).isDefined) {
          List[O](b)
        } else {
          b :: acc
        }
      }}
    }

  def readWithTimeout[F[_]](socket: Socket[F],
                            timeout: FiniteDuration,
                            shallTimeout: F[Boolean],
                            chunkSize: Int
                           )(implicit F: Effect[F]) : Stream[F, Byte] = {
    def go(remains:FiniteDuration): Stream[F, Byte] = {
      eval(shallTimeout).flatMap { shallTimeout =>
        if (!shallTimeout) {
          socket.reads(chunkSize, None)
        } else {
          if (remains <= 0.millis) {
            Stream.raiseError(new TimeoutException())
          } else {
            eval(F.delay(System.currentTimeMillis())).flatMap { start =>
              eval(socket.read(chunkSize, Some(remains))).flatMap { read =>
                eval(F.delay(System.currentTimeMillis())).flatMap { end => read match {
                  case Some(bytes) =>
                    Stream.chunk(bytes) ++ go(remains - (end - start).millis)
                  case None => Stream.empty
                }}}}
          }
        }
      }
    }

    go(timeout)
  }
}

sealed trait SubscriptionState
case object Subscribed extends SubscriptionState
case object Unsubscribed extends SubscriptionState

class State {
  import collection.mutable

  private val state = mutable.Map[String, Map[String, SubscriptionState]]()
  private val rwlock = new ReentrantReadWriteLock()

  def exchangeExists(exchange: String): Boolean = {
    withLock(rwlock.readLock()) {
      state.get(exchange) match {
        case Some(map) => map.exists { case (_, s) => s == Subscribed }
        case None => false
      }
    }
  }

  def hostState(exchange: String, host: String): Option[SubscriptionState] = {
    withLock(rwlock.readLock()) {
      state.get(exchange) match {
        case Some(map) => map.get(host)
        case None => None
      }
    }
  }

  def add(host: String, exchanges: Array[String]): Unit = {
    withLock(rwlock.writeLock()) {
      exchanges.foreach { exchange =>
        state.get(exchange) match {
          case Some(s) => state.update(exchange, s + (host -> Subscribed))
          case None => state(exchange) = Map(host -> Subscribed)
        }
      }
    }
  }

  def deleteExchanges(host: String, exchanges: Array[String]): Unit = {
    withLock(rwlock.writeLock()) {
      exchanges.foreach { exchange =>
        state.get(exchange) match {
          case Some(s) => state.update(exchange, s - host)
          case None => ()
        }
      }
    }
  }

  def unsubscribeExchanges(host: String, exchanges: Array[String]): Unit = {
    withLock(rwlock.writeLock()) {
      exchanges.foreach { exchange =>
        state.get(exchange) match {
          case Some(s) => state.update(exchange, s.updated(host, Unsubscribed))
          case None => ()
        }
      }
    }
  }

  def unsubscribeHost(host: String): Unit = {
    withLock(rwlock.writeLock()) {
      state.keys.foreach { exchange =>
        state.get(exchange) match {
          case Some(s) => state.update(exchange, s.updated(host, Unsubscribed))
          case None => ()
        }
      }
    }
  }

  def deleteHost(host: String): Unit = {
    withLock(rwlock.writeLock()) {
      state.keys.foreach { exchange =>
        state.get(exchange) match {
          case Some(s) => state.update(exchange, s - host)
          case None => ()
        }
      }
    }
  }

  def withLock[A](lock: Lock)(action: => A): A = {
    lock.lock()
    try {
      action
    } finally {
      lock.unlock()
    }
  }
}
