import cats.effect.Effect
import fs2.{Chunk, Pipe, Stream, async}

import scala.concurrent.ExecutionContext

import fs2.async.mutable.Signal

class EventSink[F[_]](state: State)(implicit F: Effect[F], ec: ExecutionContext) {
  def sinkSubscriptions(topic: async.mutable.Topic[F, Event]): Pipe[F, Subscription[F], Stream[F, Either[Throwable, Unit]]] =
    _.filter(_.socket != null).map(s =>
      Stream.eval(async.signalOf(false)).flatMap { sig =>
        topic.subscribe(100)
          .filter(_ != DummyEvent)
          .filter(_.time >= s.time)
          .filter {
            case MessageEvent(channel, _, _) => state.hostState(channel, s.addr).isDefined
            case UnsubscribeEvent(host, _, _) => host == s.addr
            case ExitEvent(host, _) => host == s.addr
          }
          .evalMap[Unit](handleEvent(_, s, sig))
          .attempt
          .interruptWhen(sig)
          .onFinalize(F.delay { state.deleteHost(s.addr) })
      }
    )

  def handleEvent(e: Event, s: Subscription[F], signal: Signal[F, Boolean]): F[Unit] = {
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
        F.flatMap(s.socket.write(Chunk.bytes("bye\n".getBytes))) { _ =>
          F.flatMap(s.socket.close) { _ =>
            signal.set(true)
          }
        }
    }
  }
}
