import cats.effect.{Concurrent, Effect}
import fs2.{Chunk, Pipe, Stream, concurrent => async}

import scala.concurrent.ExecutionContext

class EventSink[F[_]](
  implicit F: Effect[F],
  C: Concurrent[F],
  ec: ExecutionContext) {
  def sinkSubscriptions(topic: async.Topic[F, Event]): Pipe[F, Subscription[F], Stream[F, Either[Throwable, Unit]]] =
    _.filter(_.socket != null).map(s =>
      topic.subscribe(100)
        .filter(_ != DummyEvent)
        .filter(_.time >= s.time)
        .filter {
          case MessageEvent(responders, _, _) => responders.contains(s.addr)
          case UnsubscribeEvent(host, _, _) => host == s.addr
        }
        .evalMap(handleEvent(_, s, s.signal))
        .attempt
        .interruptWhen(s.signal)
    )

  def handleEvent(e: Event, s: Subscription[F], signal: async.SignallingRef[F, Boolean]): F[Unit] = {
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
