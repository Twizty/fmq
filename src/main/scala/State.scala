import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

import scala.collection.Map

sealed trait SubscriptionState
case object Subscribed extends SubscriptionState
case object Unsubscribed extends SubscriptionState

class State {
  import collection.mutable

  private val state = mutable.Map[String, Map[String, SubscriptionState]]()
  private val rwlock = new ReentrantReadWriteLock()

  def exchangeExists(exchange: String): Boolean = {
    withLock(rwlock.readLock()) {
      state.get(exchange) match {
        case Some(map) => map.exists { case (_, s) => s == Subscribed }
        case None => false
      }
    }
  }

  def hostState(exchange: String, host: String): Option[SubscriptionState] = {
    withLock(rwlock.readLock()) {
      state.get(exchange) match {
        case Some(map) => map.get(host)
        case None => None
      }
    }
  }

  def add(host: String, exchanges: Array[String]): Unit = {
    withLock(rwlock.writeLock()) {
      exchanges.foreach { exchange =>
        state.get(exchange) match {
          case Some(s) => state.update(exchange, s + (host -> Subscribed))
          case None => state(exchange) = Map(host -> Subscribed)
        }
      }
    }
  }

  def deleteExchanges(host: String, exchanges: Array[String]): Unit = {
    withLock(rwlock.writeLock()) {
      exchanges.foreach { exchange =>
        state.get(exchange) match {
          case Some(s) => state.update(exchange, s - host)
          case None => ()
        }
      }
    }
  }

  def unsubscribeExchanges(host: String, exchanges: Array[String]): Unit = {
    withLock(rwlock.writeLock()) {
      exchanges.foreach { exchange =>
        state.get(exchange) match {
          case Some(s) => state.update(exchange, s.updated(host, Unsubscribed))
          case None => ()
        }
      }
    }
  }

  def unsubscribeHost(host: String): Unit = {
    withLock(rwlock.writeLock()) {
      state.keys.foreach { exchange =>
        state.get(exchange) match {
          case Some(s) => state.update(exchange, s.updated(host, Unsubscribed))
          case None => ()
        }
      }
    }
  }

  def deleteHost(host: String): Unit = {
    withLock(rwlock.writeLock()) {
      state.keys.foreach { exchange =>
        state.get(exchange) match {
          case Some(s) => state.update(exchange, s - host)
          case None => ()
        }
      }
    }
  }

  def withLock[A](lock: Lock)(action: => A): A = {
    lock.lock()
    try {
      action
    } finally {
      lock.unlock()
    }
  }
}
