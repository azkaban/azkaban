package azkaban.reportal.util.tableau;

import java.time.Duration;

/**
 * Countdown is a class used by Tableau Job to
 * keep track of time as the Tableau extractions are
 * being refreshed.
 */
public class Countdown {

  private Duration duration;

  public Countdown(final Duration duration) {
    this.duration = duration;
  }


  public void countDownByOneMinute() throws InterruptedException {
    this.duration = this.duration.minusMinutes(1);
  }

  public boolean moreTimeRemaining() {
    return this.duration.toMillis() > 0;
  }
}
