package latis.ops

import fs2.*



object BinAverage {

  val stream = Stream(1, 2, 3, 4, 5, 6, 7, 8, 9)

  @main def run() = {
    val z: Stream[Pure, (Double, Chunk[Int])] = stream.groupAdjacentBy { n =>
      println(n / 3)
      Math.floor(n / 3) * 3
    }
    z.map(println).compile.drain
  }
}

/*
provide stats for all vars, including domain, as named tuples
but want to be able to project the stats we want, maybe as arguments

mean, min, max, count, stddev?

SQL aggregate functions: min, max, count, sum, avg
some have STDEV, VAR
https://www.tutorialspoint.com/sql/sql-aggregate-functions.htm
but no median
*/
