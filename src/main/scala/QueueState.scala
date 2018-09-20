import cats.effect.concurrent.MVar
import cats.effect.{Concurrent, Effect}
import fs2.concurrent.Queue

import scala.concurrent.ExecutionContext

class QueueState[F[_]](state: MVar[F, Map[String, Queue[F, String]]])(implicit F: Effect[F], C: Concurrent[F]) extends collection.convert.AsScalaConverters {

  def get(queue: String)(implicit ec: ExecutionContext): F[Queue[F, String]] = {
    F.flatMap(state.read) { map =>
      map.get(queue) match {
        case Some(q) =>
          F.pure(q)
        case None =>
          val newQueue = Queue.unbounded[F, String]
          F.flatMap(newQueue) { q =>
            F.flatMap(state.take) { s =>
              F.flatMap(state.put(s + (queue -> q))) { _ =>
                F.pure(q)
              }
            }
          }
      }
    }
  }
}
