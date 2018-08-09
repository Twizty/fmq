import cats.effect.Effect
import fs2.async

import scala.concurrent.ExecutionContext

class QueueState[F[_]](implicit F: Effect[F]) extends collection.convert.AsScalaConverters {
  import collection.concurrent
  import java.util.concurrent.ConcurrentHashMap

  private val state: concurrent.Map[String, async.mutable.Queue[F, String]] =
    mapAsScalaConcurrentMap(new ConcurrentHashMap[String, async.mutable.Queue[F, String]]())

  def get(queue: String)(implicit ec: ExecutionContext): F[async.mutable.Queue[F, String]] = {
    state.get(queue) match {
      case Some(q) =>
        F.pure(q)
      case None =>
        val newQueue: F[async.mutable.Queue[F, String]] = async.unboundedQueue
        F.flatMap(newQueue) { q =>
          state.putIfAbsent(queue, q) match {
            case Some(old) => F.pure(old)
            case None => F.pure(q)
          }
        }
    }
  }
}
