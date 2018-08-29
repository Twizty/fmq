import cats.effect.Effect
import fs2.{Chunk, Pipe, Stream, async}

import scala.concurrent.ExecutionContext

import fs2.async.mutable.Signal

class EventSink[F[_]](implicit F: Effect[F], ec: ExecutionContext) {
  def sinkSubscriptions(topic: async.mutable.Topic[F, Event]): Pipe[F, Subscription[F], Stream[F, Either[Throwable, Unit]]] =
    _.filter(_.socket != null).map(s =>
      topic.subscribe(100)
        .filter(_ != DummyEvent)
        .filter(_.time >= s.time)
        .filter {
          case MessageEvent(responders, _, _) => responders.contains(s.addr)
          case UnsubscribeEvent(host, _, _) => host == s.addr
        }
        .evalMap[Unit](handleEvent(_, s, s.signal))
        .attempt
        .interruptWhen(s.signal)
    )

  def handleEvent(e: Event, s: Subscription[F], signal: Signal[F, Boolean]): F[Unit] = {
    e match {
      case MessageEvent(_, payload, _) =>
        s.socket.write(Chunk.bytes(payload.getBytes))
      case UnsubscribeEvent(_, channels, _) =>
        s.socket.write(Chunk.bytes(s"unsubscribed from ${channels.mkString(", ")}".getBytes))
      case ExitEvent(_, _) =>
        signal.set(true)
    }
  }
}
