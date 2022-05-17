package data.monad

case class Reader[E, A](run: E => A) {
  def map[E2 <: E, B](f: A => B): E2 => B =
    e => f(run(e))

  def flatMap[E2 <: E, B](f: A => E2 => B): E2 => B =
    e => f(run(e))(e)
}
