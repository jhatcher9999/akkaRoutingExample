import akka.actor._

import scala.collection.immutable
import scala.util.hashing.MurmurHash3

/*

  Research/To-do:

  - routing
    https://doc.akka.io/docs/akka/current/routing.html
    http://www.tom-e-white.com/2007/11/consistent-hashing.html

  - Murmur3 hash that returns 128-bit integer:
    https://github.com/yonik/java_util/blob/master/src/util/hash/MurmurHash3.java
    Find algorithm used in Cassandra code

  - doing this across multiple machines
  - handling actor balancing
  - handling downed actors
  - dealing with items that don't come in by the key

 */

case class Work(sourceSystemName: Option[String], sourceSystemId: Option[String])

class Worker extends Actor {

  def receive: PartialFunction[Any, Unit] = {
    case w: Work =>
      println("Worker: doing work for " + w.sourceSystemName.getOrElse("") + " " + w.sourceSystemId.getOrElse(""))
      println
  }
}

class Master(workers: Array[ActorRef]) extends Actor {

  val numberOfWorkers: Int = workers.length

  def receive: PartialFunction[Any, Unit] = {
    case w: Work =>

      println("Master: processing work for item " + w.sourceSystemId.getOrElse("") + " " + w.sourceSystemName.getOrElse(""))

      //hash the input
      val hash = HashUtils.hash(w.sourceSystemId.getOrElse("") + w.sourceSystemName.getOrElse(""))
      println("Master: hash calculated: " + hash.toString)

      //find the right worker based on the hash
      val selectedWorker = route(hash, numberOfWorkers)
      val worker: ActorRef = workers(selectedWorker)
      println("Master: worker selected: " + selectedWorker.toString)

      //send work to the worker
      worker ! w

    case _ => println("Master: unexpected input")
  }

  val ranges: immutable.IndexedSeq[Int] = {
    val intMaxValue = Int.MaxValue
    val totalRange = intMaxValue.toLong * 2L + 1
    val chunkUnit = (totalRange / numberOfWorkers).toInt
    for (i <- 0 until numberOfWorkers) yield {
      Int.MinValue + (chunkUnit * i)
    }
  }

  def route(hash: Int, numberOfWorkers: Int) : Int = {

    //not sure how to do this without making this mutable (i.e., a var)
    var selection = None : Option[Int]
    for (j <- 0 until numberOfWorkers) {
      if (selection.isEmpty) {
        if (j == (numberOfWorkers - 1)) {
          selection = Some(j)
        } else {
          val currentRange = ranges(j)
          val nextRange = ranges(j + 1)

          if (hash >= currentRange && hash < nextRange) {
            selection = Some(j)
          }
        }
      }
    }
    selection.getOrElse(0)
  }

}

object HashUtils {
  def hash(input: String): Int = MurmurHash3.stringHash(input)
}

object RoutingTest extends App {

  val actorSystem = ActorSystem("RoutingSystem")

  val workers = Array(
    actorSystem.actorOf(Props[Worker], name = "worker1"),
    actorSystem.actorOf(Props[Worker], name = "worker2"),
    actorSystem.actorOf(Props[Worker], name = "worker3"),
    actorSystem.actorOf(Props[Worker], name = "worker4"),
    actorSystem.actorOf(Props[Worker], name = "worker5")
  )

  val master = actorSystem.actorOf(Props(new Master(workers)), name = "master")

  val workItems = Array(
    Work(Some("AAA"), Some("111")),
    Work(Some("BBB"), Some("222")),
    Work(Some("CCC"), Some("333")),
    Work(Some("DDD"), Some("444")),
    Work(Some("EEE"), Some("555")),
    Work(Some("FFF"), Some("666")),
    Work(Some("GGG"), Some("777")),
    Work(Some("HHH"), Some("888")),
    Work(Some("III"), Some("999")),
    Work(Some("JJJ"), Some("1000")),
    Work(Some("KKK"), None)
  )

  workItems.foreach {
    work => master ! work
  }

  //actorSystem.terminate

}