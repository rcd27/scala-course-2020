import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ClosedShape, IOResult, UniformFanOutShape}
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.Random

object Main extends App {

  def genLogs(size: Int, recordLength: Int): Seq[String] = {
    val levels = Map(0 -> "INFO", 1 -> "WARN", 2 -> "ERROR")
    (1 to size).map { _ =>
      s"${levels(Random.nextInt(3))}:${Random.alphanumeric.take(recordLength).mkString("")}\n"
    }
  }

  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>

      val source: Source[String, NotUsed] = Source(genLogs(5, 10))

      val parseLog: Flow[String, (String, String), NotUsed] = Flow[String].map({
        (a: String) => {
          val res = a.split(":")
          (res(0), res(1))
        }
      })

      val convertToByteString = Flow[(String, String)].map({
        a: (String, String) => {
          ByteString.fromString(a._2)
        }
      })

      def filterFlow(level: String) = Flow[(String, String)].filter({
        case (l: String, _) =>
          l.contains(level)
      })

      val broadcast: UniformFanOutShape[(String, String), (String, String)] = builder.add(Broadcast[(String, String)](3))

      def fileIo(fileName: String): Sink[ByteString, Future[IOResult]] = {
        FileIO.toPath(Paths.get(fileName))
      }

      source ~> parseLog ~> broadcast ~> filterFlow("INFO") ~> convertToByteString ~> fileIo("INFO")
      broadcast ~> filterFlow("WARN") ~> convertToByteString ~> fileIo("WARN")
      broadcast ~> filterFlow("ERROR") ~> convertToByteString ~> fileIo("ERROR")

      ClosedShape
    })

  implicit val as: ActorSystem = ActorSystem()
  graph.run()
}
