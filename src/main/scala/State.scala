class State extends collection.convert.AsScalaConverters {
  import collection.concurrent
  import java.util.concurrent.ConcurrentHashMap

  private val state: concurrent.Map[String, Set[String]] =
    mapAsScalaConcurrentMap(new ConcurrentHashMap[String, Set[String]]())

  def get(exchange: String): Option[Set[String]] =
    state.get(exchange)

  def add(host: String, exchanges: Array[String]): Unit =
    exchanges.foreach { exchange =>
      change(exchange, hosts => hosts + host)
    }

  def deleteExchanges(host: String, exchanges: Array[String]): Unit =
    exchanges.foreach { exchange =>
      change(exchange, hosts => hosts - host)
    }

  def deleteHost(host: String): Unit =
    state.keys.foreach { exchange =>
      change(exchange, hosts => hosts - host)
    }

  private def change(k: String, callback: Set[String] => Set[String]): Unit =
    state.get(k) match {
      case Some(value) =>
        replace(k, value, callback)
      case None =>
        state.putIfAbsent(k, callback(Set.empty)) match {
          case Some(value) => replace(k, value, callback)
          case None => ()
        }
    }

  private def replace(k: String, value: Set[String], callback: Set[String] => Set[String]): Unit = {
    if (state.replace(k, value, callback(value))) {
      ()
    } else {
      change(k, callback)
    }
  }
}

