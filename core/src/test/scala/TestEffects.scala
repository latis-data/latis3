
import cats.Id
import cats.effect._
import fs2._

//resolver
trait R[F[_]]

//case class Rio(v: Int) extends R[IO] {
//  //def foo: IO[Int] = Sync[IO].delay(v)
//  def bar: Stream[IO, Int] = Stream.eval(IO(v))
//}
//
//case class Rpure(v: Int) extends R[Id] {
//  //def foo: Id[Int] = v
//  def bar: Stream[Pure, Int] = Stream.emit(v)
//}
//
////adapter
//case class A[F[_]]()(implicit r: R[F]) {
//  def s: Stream[F, Int] = r.bar
//}
//
//
//
//object TestEffects extends App {
//  implicit val r = Rio
//  val a = A()
//  val z = a.s
//}

case class D[+F[_]](s: Stream[F, Int])

object Foo extends App {
  
//  val s: Stream[Pure, Int] = Stream.emits(Seq(1, 2, 3))
//  val d: D[Pure] = D(s)
//  //write[Pure](d)
//  val z = doIt2[IO](d)
//  z.unsafeRunSync
//  
//  def encode[F[_]](d: D[F]): Stream[F, String] = 
//    d.s.map(_.toString)
//  
//  
//  def doIt2[F[_]: Sync](d: D[F]): F[Unit] = {
//    val z = encode(d)
//    z.flatMap(x => Stream.eval(Sync[F].delay( println(x) ))).compile.drain
//  }
//  
//  def doIt[F[_]: Sync, A](s: Stream[F, A]): F[Unit] = s.flatMap(x => 
//      Stream.eval(Sync[F].delay(println(x)))).compile.drain
      

      
}







