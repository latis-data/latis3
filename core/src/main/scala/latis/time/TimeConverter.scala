package latis.time

import java.time.LocalDate
import java.time.ZoneOffset
import java.util.Date

import scala.collection.immutable.SortedMap

import latis.time.TimeScaleType._
import latis.units.UnitConverter

/**
 * The TimeConverter is a UnitConverter that converts values from one TimeScale
 * to another.
 *
 * This accounts for leap second adjustments when dealing with atomic TimeScales.
 * Civil time scales, which do not count leap seconds, are aligned with the
 * default Java time scale, which simply ignores leap seconds. Civil time
 * scales effectively pause during a leap second. When converting with atomic
 * time scales, which do count leap seconds, we must make adjustments to
 * account for the different accounting of leap seconds.
 *
 * Note that this assumes zero leap seconds before 1972, when UTC was defined,
 * then starting with 10 leap seconds at 1970:01:01T00:00:00.
 */
case class TimeConverter(ts1: TimeScale, ts2: TimeScale) extends UnitConverter(ts1, ts2) {

  import TimeConverter._

  /**
   * Converts the given time from the first TimeScale to the second
   * with a leaps second adjustment as needed.
   */
  override def convert(time: Double): Double = _convert(time)

  /**
   * Defines a function that converts a time from ts1 to ts2.
   *
   * This only invokes leap second configuration if there is a Atomic TimeScale
   * involved.
   */
  private val _convert: Double => Double = {
    if (ts1.timeScaleType == Atomic || ts2.timeScaleType == Atomic)
      (time: Double) => super.convert(time) + leapSecondAdjustment(time)
    else (time: Double) => super.convert(time)
  }

  /**
   * Returns the leap second adjustment for the given time.
   *
   * The given time is expected to be in the units of the input TimeScale.
   * The resulting adjustment will be in the units of the target TimeScale.
   */
  private def leapSecondAdjustment(time: Double): Double =
    leapSecondAdjustmentMap.rangeTo(time).lastOption.map(_._2).get

  /**
   * Constructs a sorted map from the time of a leap second in the first TimeScale
   * to the leap second adjustment in the units of the second TimeScale.
   *
   * This is designed to optimize the lookup of leap second adjustments
   * for repeated conversions performed by this TimeConverter.
   */
  private lazy val leapSecondAdjustmentMap: SortedMap[Double, Double] = {
    // This is lazy since it is not needed by all conversions AND
    //   it prevents a stack overflow since the internal TimeConverter
    //   would in turn try to construct this map. Because this is lazy,
    //   the loop will short-circuit when converting the UTC date to a
    //   civil time scale which does not need a leap second adjustment.
    // Note that this adds an adjustment for any time before 1972.

    // Function to convert a Date to the incoming TimeScale
    val convertDate: Date => Double = {
      val converter = TimeConverter(TimeScale.Default, ts1)
      (date: Date) => converter.convert(date.getTime.toDouble)
    }

    (leapSeconds.map { case (date, ls) =>
      (convertDate(date), computeAdjustment(ls))
    }).concat(List((Double.MinValue, computeAdjustment(0))))
  }

  /**
   * Computes the leap second adjustment based on the TimeScale types.
   *
   * The given leap second count is combined with leap second counts at
   * TimeScale epochs as needed. The adjustment is converted to the target
   * TimeScale's units.
   */
  private def computeAdjustment(ls: Int): Double =
    (ts1.timeScaleType, ts2.timeScaleType) match {
      // leap second difference between atomic epochs
      case (TimeScaleType.Atomic, TimeScaleType.Atomic) =>
        (getLeapSeconds(ts1.epoch) - getLeapSeconds(ts2.epoch)).toDouble / ts2.baseMultiplier
      // subtract leap seconds before target atomic epoch
      case (TimeScaleType.Civil, TimeScaleType.Atomic) =>
        (ls - getLeapSeconds(ts2.epoch)).toDouble / ts2.baseMultiplier
      // subtract leap seconds on incoming atomic TimeScale
      case (TimeScaleType.Atomic, TimeScaleType.Civil) =>
        (getLeapSeconds(ts1.epoch) - ls).toDouble / ts2.baseMultiplier
      case _ => 0d
    }
}

object TimeConverter {

  /**
   * Returns the accumulated number of leap seconds for a given Date.
   *
   * This takes advantage of the ordering provided by a SortedMap to find the
   * most recent (previous) entry. This will return 0 for times before 1972.
   */
  def getLeapSeconds(date: Date): Int =
    leapSeconds.rangeTo(date).lastOption.map(_._2).getOrElse(0)

  /**
   * Defines a SortedMap with an entry for all leap seconds, mapping the Date
   * of a leap second to the accumulated count of leap seconds at that Date.
   */
  val leapSeconds: SortedMap[Date, Int] = SortedMap(
    ("1972-01-01", 10),
    ("1972-07-01", 11),
    ("1973-01-01", 12),
    ("1974-01-01", 13),
    ("1975-01-01", 14),
    ("1976-01-01", 15),
    ("1977-01-01", 16),
    ("1978-01-01", 17),
    ("1979-01-01", 18),
    ("1980-01-01", 19),
    ("1981-07-01", 20),
    ("1982-07-01", 21),
    ("1983-07-01", 22),
    ("1985-07-01", 23),
    ("1988-01-01", 24),
    ("1990-01-01", 25),
    ("1991-01-01", 26),
    ("1992-07-01", 27),
    ("1993-07-01", 28),
    ("1994-07-01", 29),
    ("1996-01-01", 30),
    ("1997-07-01", 31),
    ("1999-01-01", 32),
    ("2006-01-01", 33),
    ("2009-01-01", 34),
    ("2012-07-01", 35),
    ("2015-07-01", 36),
    ("2017-01-01", 37)
  ).map { case (date, ls) =>
    (new Date(LocalDate.parse(date).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000), ls)
  }
}
