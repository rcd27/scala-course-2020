import cats.effect.{Blocker, ExitCode, IO, IOApp, Resource}
import doobie._
import doobie.implicits._
import doobie.h2._
import doobie.util.ExecutionContexts

object DoobieMain extends IOApp {

  // Resource yielding a transactor configured with a bounded connect EC and an unbounded
  // transaction EC. Everything will be closed and shut down cleanly after use.
  val transactor: Resource[IO, H2Transactor[IO]] =
  for {
    ce <- ExecutionContexts.fixedThreadPool[IO](32) // our connect EC
    be <- Blocker[IO]    // our blocking EC
    xa <- H2Transactor.newH2Transactor[IO](
      "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", // connect URL
      "sa",                                   // username
      "",                                     // password
      ce,                                     // await connection here
      be                                      // execute JDBC operations here
    )
  } yield xa


  def run(args: List[String]): IO[ExitCode] =
    transactor.use { xa =>

      // Construct and run your server here!
      for {
        n <- sql"select 42".query[Int].unique.transact(xa)
        _ <- IO(println(n))
        _ <- createTables.transact(xa)
      } yield ExitCode.Success

    }

  def createTables = {
    val createPerson =
      sql"""
    CREATE TABLE person (
      id   SERIAL,
      name VARCHAR NOT NULL UNIQUE,
      age  SMALLINT,
      salary INT,
      depId INT
    )
  """.update.run

    val createDepartment =
      sql"""
        CREATE TABLE department (
        id INT,
        name VARCHAR NOT NULL UNIQUE)
        """.update.run
    for {
      _ <- createPerson
      _ <- createDepartment
    } yield ()
  }

}