import scala.concurrent.duration
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

sealed trait Command
case class Publish(channel: String, data: String) extends Command
case class Subscribe(channels: Array[String]) extends Command
case class Unsubscribe(channels: Array[String]) extends Command
case class Pull(name: String, timeout: Option[FiniteDuration]) extends Command
case class Push(name: String, data: String) extends Command
case object Exit extends Command

object CommandParser {
  val pattern: Regex = "(pull|push|publish|subscribe|unsubscribe|exit)( [\\w,]+| \"[\\w\\ ]+\")?( .+)?\r?\n?".r

  def parse(q: String): Option[Command] = {
    q match {
      case pattern("publish", channel, data) if channel != null && data != null =>
        Some(Publish(channel.trim, data.trim))
      case pattern("subscribe", channels, _) if channels != null =>
        Some(Subscribe(channels.trim.split(",")))
      case pattern("unsubscribe", channels, _) if channels != null =>
        Some(Unsubscribe(channels.trim.split(",")))
      case pattern("push", name, data) if name != null && data != null =>
        Some(Push(name.trim, data.trim))
      case pattern("push", name, data) if name != null && data != null =>
        Some(Push(name.trim, data.trim))
      case pattern("pull", name, timeout) if name != null && timeout != null =>
        Some(Pull(name.trim, safe { timeout.trim.toLong }.map(FiniteDuration(_, duration.MILLISECONDS))))
      case pattern("pull", name, null) if name != null =>
        Some(Pull(name.trim, None))
      case pattern("exit", _, _) =>
        Some(Exit)
      case _ =>
        None
    }
  }

  def safe[A](action: => A): Option[A] = {
    try {
      Some(action)
    } catch {
      case _: Exception => None
    }
  }
}
