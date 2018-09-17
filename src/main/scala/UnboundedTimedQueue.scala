import cats.effect.{Concurrent, ExitCase, Timer}
import fs2.{Chunk, Pipe, Stream}
import fs2.concurrent.Queue
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._

import scala.concurrent.duration.FiniteDuration

final class Token extends Serializable {
  override def toString: String = s"Token(${hashCode.toHexString})"
}


final case class QState[F[_], A](
  queue: Vector[A],
  deq: Vector[(Token, Deferred[F, Chunk[A]])],
  peek: Option[Deferred[F, A]]
)

object UnboundedTimedQueue {
  def apply[F[_], A](implicit F: Concurrent[F]): F[UnboundedTimedQueue[F, A]] =
    Ref
      .of[F, QState[F, A]](QState(Vector.empty, Vector.empty, None))
      .map(new UnboundedTimedQueue(_))
}

class UnboundedTimedQueue[F[_], A](qref: Ref[F, QState[F, A]])(
  implicit F: Concurrent[F]
)
  extends Queue[F, A] {
  type Getter = (Deferred[F, Chunk[A]], F[Unit]) => F[Chunk[A]]

  protected def sizeChanged(s: QState[F, A], ns: QState[F, A]): F[Unit] = F.unit

  def enqueue1(a: A): F[Unit] = offer1(a).void
  def timedEnqueue1(a: A, finiteDuration: FiniteDuration)(implicit T: Timer[F]): F[Boolean] =
    offer1(a).as(true)

  def offer1(a: A): F[Boolean] =
    qref
      .modify { s =>
        val (newQState, signalDequeuers) = s.deq match {
          case dequeuers if dequeuers.isEmpty =>
            // we enqueue a value to the queue
            val ns = s.copy(queue = s.queue :+ a, peek = None)
            ns -> sizeChanged(s, ns)
          case (_, firstDequeuer) +: dequeuers =>
            // we await the first dequeuer
            s.copy(deq = dequeuers, peek = None) -> F.start {
              firstDequeuer.complete(Chunk.singleton(a))
            }.void
        }

        val signalPeekers =
          s.peek.fold(F.unit)(p => F.start(p.complete(a)).void)

        newQState -> (signalDequeuers *> signalPeekers)
      }
      .flatten
      .as(true)

  def dequeue1: F[A] = dequeueBatch1(1).map(_.head.get)
  def timedDequeue1(duration: FiniteDuration)(implicit T: Timer[F]): F[Option[A]] = timedDequeueBatch1(1, duration).map(_.head)

  def dequeue: Stream[F, A] =
    Stream
      .bracket(F.delay(new Token))(t =>
        qref.update(s => s.copy(deq = s.deq.filterNot(_._1 == t))))
      .flatMap(t => Stream.repeatEval(dequeueBatch1Impl(1, t, (d, _) => d.get).map(_.head.get)))

  def dequeueBatch: Pipe[F, Int, A] =
    batchSizes =>
      Stream
        .bracket(F.delay(new Token))(t =>
          qref.update(s => s.copy(deq = s.deq.filterNot(_._1 == t))))
        .flatMap(t =>
          batchSizes.flatMap(batchSize =>
            Stream.eval(dequeueBatch1Impl(batchSize, t, (d, _) => d.get)).flatMap(Stream.chunk(_))))

  def dequeueBatch1(batchSize: Int): F[Chunk[A]] =
    dequeueBatch1Impl(batchSize, new Token, (d, _) => d.get)

  def timedDequeueBatch1(batchSize: Int, duration: FiniteDuration)(implicit T: Timer[F]): F[Chunk[A]] = {
    val getter: Getter = (d, cleanup) =>
      F.flatMap(F.race(T.sleep(duration), d.get)) {
        case Left(_) =>
          cleanup *> d.complete(Chunk.empty[A]).attempt.flatMap {
            case Left(_) => d.get
            case Right(_) => Chunk.empty[A].pure[F]
          }
        case Right(v) => v.pure[F]
      }
    dequeueBatch1Impl(batchSize, new Token, getter)
  }

  private def dequeueBatch1Impl(batchSize: Int, token: Token, getter: Getter): F[Chunk[A]] =
    Deferred[F, Chunk[A]].flatMap { d =>
      qref.modify { s =>
        val newState =
          if (s.queue.isEmpty) s.copy(deq = s.deq :+ (token -> d))
          else s.copy(queue = s.queue.drop(batchSize))

        val cleanup =
          if (s.queue.nonEmpty) F.unit
          else qref.update(s => s.copy(deq = s.deq.filterNot(_._2 == d)))

        val dequeueBatch = sizeChanged(s, newState).flatMap { _ =>
          if (s.queue.nonEmpty) {
            if (batchSize == 1) Chunk.singleton(s.queue.head).pure[F]
            else Chunk.indexedSeq(s.queue.take(batchSize)).pure[F]
          } else {
            F.guaranteeCase(getter(d, cleanup)) {
              case ExitCase.Completed => F.unit
              case ExitCase.Error(t)  => cleanup *> F.raiseError(t)
              case ExitCase.Canceled  => cleanup *> F.unit
            }
          }
        }

        newState -> dequeueBatch
      }.flatten
    }
}
