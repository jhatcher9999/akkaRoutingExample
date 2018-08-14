import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.{ConsistentHashingGroup, ConsistentHashingPool}
import akka.routing.ConsistentHashingRouter.{ConsistentHashMapping, ConsistentHashable}

case class WorkItem(sourceSystemName: Option[String], sourceSystemId: Option[String]) extends ConsistentHashable {
  override def consistentHashKey: Any = sourceSystemName.getOrElse("") + "|" + sourceSystemId.getOrElse("")
}

class WorkerActor() extends Actor {
  def receive: PartialFunction[Any, Unit] = {
    case workItem: WorkItem =>
      println("Routee: processing " + workItem.consistentHashKey + " by routee " + this.self.path)
    case _ =>
      println("Routee: unknown message")
  }
}

object WorkItemHelper {
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
}

object RoutingConsistentHashPoolTest extends App {

  val actorSystem = ActorSystem("RoutingConsistentHashPoolSystem")

  ////// POOL ROUTER
  //in the pool router, we let the router control the routees

  println("Processing work items with Pool Router")

  val poolRouter = actorSystem.actorOf(
    ConsistentHashingPool(
      nrOfInstances = 5
    ).props(Props(new WorkerActor())),
    name = "routerMapping"
  )

  //process the work items
  WorkItemHelper.workItems.foreach {
    workItem => poolRouter ! workItem
  }
  //process them again to see that work items were processed by consistent routees
  WorkItemHelper.workItems.foreach {
    workItem => poolRouter ! workItem
  }

  //actorSystem.terminate()

}

object RoutingConsistentHashGroupTest extends App {

  val actorSystem = ActorSystem("RoutingConsistentHashGroupSystem")

  ////// GROUP ROUTER
  //in the group router, we control the routees outside of the router

  println("Processing work items with Group Router")

  val routees = Seq(
    actorSystem.actorOf(Props(new WorkerActor())),
    actorSystem.actorOf(Props(new WorkerActor())),
    actorSystem.actorOf(Props(new WorkerActor())),
    actorSystem.actorOf(Props(new WorkerActor())),
    actorSystem.actorOf(Props(new WorkerActor()))
  )

  val paths = routees.map(routee => routee.path.toSerializationFormat).toList

  val groupRouter = actorSystem.actorOf(
    ConsistentHashingGroup(paths).props(),
    name = "routerMapping2"
  )

  //process the work items
  WorkItemHelper.workItems.foreach {
    workItem => groupRouter ! workItem
  }
  //process them again to see that work items were processed by consistent routees
  WorkItemHelper.workItems.foreach {
    workItem => groupRouter ! workItem
  }

  //actorSystem.terminate()

}