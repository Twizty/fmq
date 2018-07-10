sealed trait Command
case class Push(channel: String, data: String) extends Command
case class Subscribe(channels: Array[String]) extends Command
case class Unsubscribe(channels: Array[String]) extends Command
case object Exit extends Command

object CommandParser {
  def parse(q: String): Option[Command] = {
    q.split(' ') match {
      case Array(f, s, t) if f == "push" => Some(Push(s, t.trim)) // TODO: fix parsing
      case Array(f, s) if f == "subscribe" => Some(Subscribe(s.trim.split(',')))
      case Array(f, s) if f == "unsubscribe" => Some(Unsubscribe(s.trim.split(',')))
      case a if a.length >= 1 && a(0).startsWith("exit") => Some(Exit)
      case _ =>
        None
    }
  }
}
