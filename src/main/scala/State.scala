sealed trait SubscriptionState
case object Subscribed extends SubscriptionState
case object Unsubscribed extends SubscriptionState

class State extends collection.convert.AsScalaConverters {
  import collection.concurrent
  import java.util.concurrent.ConcurrentHashMap

  private val state: concurrent.Map[String, concurrent.Map[String, SubscriptionState]] =
    mapAsScalaConcurrentMap(new ConcurrentHashMap[String, concurrent.Map[String, SubscriptionState]]())

  def newMap(k: String, v: SubscriptionState) = {
    val m = mapAsScalaConcurrentMap(new ConcurrentHashMap[String, SubscriptionState]())
    m.put(k, v)
    m
  }

  def exchangeExists(exchange: String): Boolean =
    state.get(exchange) match {
      case Some(map) => map.exists { case (_, s) => s == Subscribed }
      case None => false
    }

  def hostState(exchange: String, host: String): Option[SubscriptionState] =
    state.get(exchange) match {
      case Some(map) => map.get(host)
      case None => None
    }

  def add(host: String, exchanges: Array[String]): Unit =
    exchanges.foreach { exchange =>
      state.get(exchange) match {
        case Some(s) => s.put(host, Subscribed)
        case None =>
          state.putIfAbsent(exchange, newMap(host, Subscribed)) match {
            case Some(old) => old.put(host, Subscribed)
            case None => ()
          }
      }
    }

  def deleteExchanges(host: String, exchanges: Array[String]): Unit =
    exchanges.foreach { exchange =>
      state.get(exchange) match {
        case Some(s) => s.remove(host)
        case None => ()
      }
    }

  def unsubscribeExchanges(host: String, exchanges: Array[String]): Unit =
    exchanges.foreach { exchange =>
      state.get(exchange) match {
        case Some(s) => s.update(host, Unsubscribed)
        case None => ()
      }
    }

  def unsubscribeHost(host: String): Unit =
    state.keys.foreach { exchange =>
      state.get(exchange) match {
        case Some(s) => s.update(host, Unsubscribed)
        case None => ()
      }
    }

  def deleteHost(host: String): Unit =
    state.keys.foreach { exchange =>
      state.get(exchange) match {
        case Some(s) => s.remove(host)
        case None => ()
      }
    }
}

