import akka.actor.{Actor, ActorSystem, Props}

class HelloActor(myName: String) extends Actor {

  def receive: PartialFunction[Any, Unit] = {
    case "hello" => println("hello from " + myName)
    case _ => println("'huh?', said " + myName)
  }

}

object HelloTest extends App {

  val system = ActorSystem("HelloSystem")

  val helloActor = system.actorOf(Props(new HelloActor("Fred")), name = "helloactor")

  helloActor ! "hello"
  helloActor ! "buenos dias"

  system.terminate

}