import scala.util.matching.Regex

sealed trait Command
case class Push(channel: String, data: String) extends Command
case class Subscribe(channels: Array[String]) extends Command
case class Unsubscribe(channels: Array[String]) extends Command
case object Exit extends Command

object CommandParser {
  val pattern: Regex = "(push|subscribe|unsubscribe|exit)( [\\w,]+| \"[\\w\\ ]+\")?( .+)?\r?\n?".r

  def parse(q: String): Option[Command] = {
    q match {
      case pattern("push", channel, data) if channel != null && data != null =>
        Some(Push(channel.trim, data.trim))
      case pattern("subscribe", channels, _) if channels != null =>
        Some(Subscribe(channels.trim.split(",")))
      case pattern("unsubscribe", channels, _) if channels != null =>
        Some(Unsubscribe(channels.trim.split(",")))
      case pattern("exit", _, _) =>
        Some(Exit)
      case _ =>
        None
    }
  }
}
