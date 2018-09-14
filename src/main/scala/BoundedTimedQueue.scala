import cats.effect.{Concurrent, Timer}
import cats.effect.concurrent.{Ref, Semaphore}
import fs2.{Chunk, Pipe, Stream}
import fs2.concurrent.Queue
import cats.implicits._

import scala.concurrent.duration.FiniteDuration

class BoundedTimedQueue[F[_], A](permits: Semaphore[F], q: UnboundedTimedQueue[F, A])(
  implicit F: Concurrent[F])
  extends Queue[F, A] {
  def timedEnqueue1(a: A, duration: FiniteDuration)(implicit T: Timer[F]): F[Boolean] =
    Ref.of(false).flatMap { r =>
      F.start(permits.acquire *> q.enqueue1(a) *> r.set(true)).flatMap[Boolean] { fiber =>
        F.race(fiber.join, T.sleep(duration)).flatMap[Boolean] {
          case Left(_) => true.pure[F]
          case Right(_) => F.flatMap(fiber.cancel)(_ => r.get)
        }
      }
    }

  def enqueue1(a: A): F[Unit] =
    permits.acquire *> q.enqueue1(a)
  def offer1(a: A): F[Boolean] =
    permits.tryAcquire.flatMap { b =>
      if (b) q.offer1(a) else F.pure(false)
    }
  def dequeue1: F[A] = dequeueBatch1(1).map(_.head.get)
  def dequeue: Stream[F, A] = q.dequeue.evalMap(a => permits.release.as(a))
  def dequeueBatch1(batchSize: Int): F[Chunk[A]] =
    q.dequeueBatch1(batchSize).flatMap { chunk =>
      permits.releaseN(chunk.size).as(chunk)
    }
  def dequeueBatch: Pipe[F, Int, A] =
    q.dequeueBatch.andThen(_.chunks.flatMap(c =>
      Stream.eval(permits.releaseN(c.size)).flatMap(_ => Stream.chunk(c))))
}