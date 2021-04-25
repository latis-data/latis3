package latis.output

import cats.Functor
import fs2.Stream

import latis.dataset.Dataset

trait Encoder[F[_], O] {

  /** Convert a `Dataset` into a `Stream` of `O`. */
  def encode(dataset: Dataset): Stream[F, O]
}

object Encoder {

  implicit def encoderFunctor[F[_]]: Functor[Encoder[F, *]] =
    new Functor[Encoder[F, *]] {
      override def map[A, B](fa: Encoder[F, A])(f: A => B): Encoder[F, B] =
        new Encoder[F, B] {
          override def encode(dataset: Dataset): Stream[F, B] =
            fa.encode(dataset).map(f)
        }
    }
}
