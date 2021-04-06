package latis.output

import fs2.Stream

import latis.dataset.Dataset

trait Encoder[F[_], O] {
  /** Convert a `Dataset` into a `Stream` of `O`. */
  def encode(dataset: Dataset): Stream[F, O]
}
