import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.{ConsistentHashingGroup, ConsistentHashingPool}
import akka.routing.ConsistentHashingRouter.{ConsistentHashMapping, ConsistentHashable}

case class WorkItem(sourceSystemName: Option[String], sourceSystemId: Option[String]) extends ConsistentHashable {
  override def consistentHashKey: Any = sourceSystemName.getOrElse("") + "|" + sourceSystemId.getOrElse("")
}

class RoutingConsistentHashPool() extends Actor {
  def receive: PartialFunction[Any, Unit] = {
    case workItem: WorkItem =>
      println("Routee: processing " + workItem.consistentHashKey + " by routee " + this.self.path)
    case _ =>
      println("Routee: unknown message")
  }
}

object RoutingConsistentHashPoolTest extends App {

  val actorSystem = ActorSystem("RoutingConsistentHashPoolSystem")

  val workItems = Array(
    WorkItem(Some("AAA"), Some("111")),
    WorkItem(Some("BBB"), Some("222")),
    WorkItem(Some("CCC"), Some("333")),
    WorkItem(Some("DDD"), Some("444")),
    WorkItem(Some("EEE"), Some("555")),
    WorkItem(Some("FFF"), Some("666")),
    WorkItem(Some("GGG"), Some("777")),
    WorkItem(Some("HHH"), Some("888")),
    WorkItem(Some("III"), Some("999")),
    WorkItem(Some("JJJ"), Some("1000")),
    WorkItem(Some("KKK"), None)
  )

  //as an alternative to adding a consistentHashKey method to WorkItem, you could create a method here and pass it to the router
  //  def hashMapping: ConsistentHashMapping = {
  //    case workItem: WorkItem => workItem.sourceSystemName.getOrElse("") + "|" + workItem.sourceSystemId.getOrElse("")
  //  }
  //
  //  val router = actorSystem.actorOf(
  //    ConsistentHashingPool(
  //      nrOfInstances = 5,
  //      virtualNodesFactor = 5,
  //      hashMapping = hashMapping
  //    ).props(Props(new RoutingConsistentHashPool())),
  //    name = "routerMapping"
  //  )

  ////// POOL ROUTER

  println("Processing work items with Pool Router")

  val router = actorSystem.actorOf(
    ConsistentHashingPool(
      nrOfInstances = 5
    ).props(Props(new RoutingConsistentHashPool())),
    name = "routerMapping"
  )

  workItems.foreach {
    workItem => router ! workItem
  }

  println

  ////// GROUP ROUTER

  println("Processing work items with Group Router")

  val routees = Seq(
    actorSystem.actorOf(Props(new RoutingConsistentHashPool())),
    actorSystem.actorOf(Props(new RoutingConsistentHashPool())),
    actorSystem.actorOf(Props(new RoutingConsistentHashPool())),
    actorSystem.actorOf(Props(new RoutingConsistentHashPool())),
    actorSystem.actorOf(Props(new RoutingConsistentHashPool()))
  )

  val paths = routees.map(routee => routee.path.toSerializationFormat).toList

  val router2 = actorSystem.actorOf(
    ConsistentHashingGroup(paths).props(),
    name = "routerMapping2"
  )

  workItems.foreach {
    workItem => router2 ! workItem
  }

  //actorSystem.terminate()

}