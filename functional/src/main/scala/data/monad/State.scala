package data.monad

import data.monad.State.unit

case class State[S, +A](run: S => (S, A)) {
  def map[B](f: A => B): State[S, B]  =
    flatMap(a => unit(f(a)))

  def map2[B, C](sb: State[S, B])(f: (A, B) => C): State[S, C] =
    flatMap(a => sb.map(b => f(a, b)))

  def flatMap[B](f: A => State[S, B]): State[S, B] = State {
    s => {
      val (s1, a) = run(s)
      f(a).run(s1)
    }
  }
}

object State {
  def unit[S, A](a: A): State[S, A] =
    State(s => (s, a))

  def get[S]: State[S, S] = State(s => (s, s))

  def set[S](s: S): State[S, Unit] = State(_ => (s, ()))

  def modify[S](f: S => S): State[S, Unit] = for {
    s <- get
    _ <- set(f(s))
  } yield ()
}