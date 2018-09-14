import cats.effect.Concurrent
import cats.effect.concurrent.MVar

class FunctionalState[F[_]](state: MVar[F, Map[String, Set[String]]])(implicit F: Concurrent[F]) {
  type State = Map[String, Set[String]]

  def get(exchange: String): F[Option[Set[String]]] =
    F.map(state.read) { map =>
      map.get(exchange)
    }

  def add(host: String, exchanges: Array[String]): F[Unit] =
    change { map =>
      exchanges.foldLeft(map) { case (acc, exchange) =>
        acc.get(exchange) match {
          case Some(set) => acc + (exchange -> (set + host))
          case None => acc + (exchange -> Set(host))
        }
      }
    }

  def deleteExchanges(host: String, exchanges: Array[String]): F[Unit] =
    change { map =>
      exchanges.foldLeft(map) { case (acc, exchange) =>
        acc.get(exchange) match {
          case Some(set) => acc + (exchange -> (set - host))
          case None => acc
        }
      }
    }

  def deleteHost(host: String): F[Unit] =
    change { map =>
      map.keys.foldLeft(map) { case (acc, exchange) =>
        acc.get(exchange) match {
          case Some(set) => acc + (exchange -> (set - host))
          case None => acc
        }
      }
    }

  private def change(f: State => State): F[Unit] =
    F.flatMap(state.take) { map =>
      state.put(f(map))
    }
}
