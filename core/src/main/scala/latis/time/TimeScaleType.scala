package latis.time

/**
 * Defines how a TimeScale deals with leap seconds.
 */
sealed trait TimeScaleType

object TimeScaleType {

  /**
   * UTC time scales are adjusted for leap seconds, effectively not counting them.
   *
   * The UTC time scale is an imperfect attempt to align civil time with atomic
   * time while remaining consistent with the rotation of the Earth. The Earth
   * has been running a bit slow (until recently) so leap seconds have been added
   * to give the UTC clock something else to do while the earth catches up.
   *
   * Due to the introduction of leap seconds, UTC is not a well-behaved scale
   * in that a discontinuity must be handled somehow. Here, UTC is modeled by
   * aligning with Unix/POSIX time as implemented in Java. It counts SI seconds
   * at the same rate as a TAI time scale while maintaining exactly 86400
   * seconds per day (instead of making the day one second longer or changing
   * the length of a second). The leap second discontinuity is handled by
   * replaying a leap second, effectively pausing the clock at midnight.
   *
   * Durations for UTC time scales will not include leap seconds.
   */
  case object UTC extends TimeScaleType

  /**
   * TAI time scales count all seconds, including leap seconds.
   *
   * TAI time scales are best for scientific use cases and spacecraft
   * operations since they uniformly count all real SI seconds. (Relativistic
   * effects are not accounted for here.) Unfortunately, many data managers
   * tend to ignore leap seconds, putting differentiation at risk.
   *
   * Due to the dependence on the underlying Java time scale and the use of UTC
   * timestamps as epochs, leap seconds must be considered when converting TAI
   * time scales to and from UTC time scales and between TAI time scales with
   * different epochs.
   */
  case object TAI extends TimeScaleType

}
