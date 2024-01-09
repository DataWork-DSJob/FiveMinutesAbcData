package flink.debug.base;


import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @projectName: JavaApiStudy
 * @className: CopyRateLimiter
 * @description: com.github.hjq.java.frame.commons.net.CopyRateLimiter
 * @author: jiaqing.he
 * @date: 2023/1/16 11:39
 * @version: 1.0
 */
public class RateLimiterCopy {

    private AbstractSleepingStopwatch stopwatch;
    private volatile Object mutexDoNotUseDirectly;
    private double maxBurstSeconds;

    RateLimiterCopy(AbstractSleepingStopwatch stopwatch, double maxBurstSeconds) {
        this.stopwatch = stopwatch;
        this.maxBurstSeconds = maxBurstSeconds;
    }

    public static RateLimiterCopy create(double permitsPerSecond) {
        return create(AbstractSleepingStopwatch.createFromSystemTimer(), permitsPerSecond);
    }

    static RateLimiterCopy create(AbstractSleepingStopwatch stopwatch, double permitsPerSecond) {
        RateLimiterCopy rateLimiter = new RateLimiterCopy(stopwatch, 1.0);
        rateLimiter.setRate(permitsPerSecond);
        return rateLimiter;
    }

    public static void checkState(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }

    static class Ticker {
        /**
         * Constructor for use by subclasses.
         */
        protected Ticker() { }

        public long read() {
            return System.nanoTime();
        };

        public static Ticker systemTicker() {
            return SYSTEM_TICKER;
        }

        private static final Ticker SYSTEM_TICKER = new Ticker() {
            @Override
            public long read() {
                return System.nanoTime();
            }
        };

    }

    static class Stopwatch {
        private final Ticker ticker;
        private boolean isRunning;
        private long elapsedNanos;
        private long startTick;

        Stopwatch() {
            this.ticker = Ticker.systemTicker();
            checkState(!isRunning, "This stopwatch is already running.");
            isRunning = true;
            startTick = ticker.read();
        }

        private long elapsedNanos() {
            return isRunning ? ticker.read() - startTick + elapsedNanos : elapsedNanos;
        }

        public long elapsed(TimeUnit desiredUnit) {
            return desiredUnit.convert(elapsedNanos(), NANOSECONDS);
        }

        @Override
        public String toString() {
            long nanos = elapsedNanos();
            return nanos + " ";
        }

    }

    abstract static class AbstractSleepingStopwatch {
        protected AbstractSleepingStopwatch() { }

        /**
         * readMicros
         *
         * @return
         */
        protected abstract long readMicros();

        /**
         * sleepMicrosUninterruptibly
         *
         * @param micros
         */
        protected abstract void sleepMicrosUninterruptibly(long micros);

        public static final AbstractSleepingStopwatch createFromSystemTimer() {
            return new AbstractSleepingStopwatch() {
                private final Stopwatch stopwatch = new Stopwatch();

                @Override
                protected long readMicros() {
                    return stopwatch.elapsed(MICROSECONDS);
                }

                @Override
                protected void sleepMicrosUninterruptibly(long micros) {
                    if (micros > 0) {
                        sleepUninterruptibly(micros, MICROSECONDS);
                    }
                }
            };
        }

        public static void sleepUninterruptibly(long sleepFor, TimeUnit unit) {
            boolean interrupted = false;
            try {
                long remainingNanos = unit.toNanos(sleepFor);
                long end = System.nanoTime() + remainingNanos;
                while (true) {
                    try {
                        // TimeUnit.sleep() treats negative timeouts just like zero.
                        NANOSECONDS.sleep(remainingNanos);
                        return;
                    } catch (InterruptedException e) {
                        interrupted = true;
                        remainingNanos = end - System.nanoTime();
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

    }

    public final void setRate(double permitsPerSecond) {
        synchronized (mutex()) {
            doSetRate(permitsPerSecond, stopwatch.readMicros());
        }
    }

    final void doSetRate(double permitsPerSecond, long nowMicros) {
        resync(nowMicros);
        double micros = SECONDS.toMicros(1L) / permitsPerSecond;
        this.stableIntervalMicros = micros;
        doSetRate(permitsPerSecond, micros);
    }

    public static final BigDecimal INIT_STATE_ZORA = BigDecimal.valueOf(0.0D);

    void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
        double oldMaxPermits = this.maxPermits;
        maxPermits = maxBurstSeconds * permitsPerSecond;
        if (oldMaxPermits == Double.POSITIVE_INFINITY) {
            // if we don't special-case this, we would get storedPermits == NaN, below
            storedPermits = maxPermits;
        } else {
            storedPermits = (BigDecimal.valueOf(oldMaxPermits).equals(INIT_STATE_ZORA))
                            ? 0.0
                            : storedPermits * maxPermits / oldMaxPermits;
        }
    }

    public double acquire() {
        return acquire(1);
    }

    public double acquire(int permits) {
        long microsToWait = reserve(permits);
        stopwatch.sleepMicrosUninterruptibly(microsToWait);
        return 1.0 * microsToWait / SECONDS.toMicros(1L);
    }

    final long reserve(int permits) {
        synchronized (mutex()) {
            return reserveAndGetWaitLength(permits, stopwatch.readMicros());
        }
    }

    final long reserveAndGetWaitLength(int permits, long nowMicros) {
        long momentAvailable = reserveEarliestAvailable(permits, nowMicros);
        return max(momentAvailable - nowMicros, 0);
    }

    private double storedPermits;
    private double maxPermits;
    private double stableIntervalMicros;
    private long nextFreeTicketMicros = 0L;

    final long reserveEarliestAvailable(int requiredPermits, long nowMicros) {
        resync(nowMicros);
        long returnValue = nextFreeTicketMicros;
        double storedPermitsToSpend = min(requiredPermits, this.storedPermits);
        double freshPermits = requiredPermits - storedPermitsToSpend;
        long waitMicros = storedPermitsToWaitTime(this.storedPermits, storedPermitsToSpend)
                        + (long) (freshPermits * stableIntervalMicros);

        this.nextFreeTicketMicros = saturatedAdd(nextFreeTicketMicros, waitMicros);
        this.storedPermits -= storedPermitsToSpend;
        return returnValue;
    }

    public static long saturatedAdd(long a, long b) {
        long naiveSum = a + b;
        if ((a ^ b) < 0 | (a ^ naiveSum) >= 0) {
            // If a and b have different signs or a has the same sign as the result then there was no
            // overflow, return.
            return naiveSum;
        }
        // we did over/under flow, if the sign is negative we should return MAX otherwise MIN
        return Long.MAX_VALUE + ((naiveSum >>> (Long.SIZE - 1)) ^ 1);
    }

    private long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
        return 0L;
    }

    void resync(long nowMicros) {
        // if nextFreeTicket is in the past, resync to now
        if (nowMicros > nextFreeTicketMicros) {
            double newPermits = (nowMicros - nextFreeTicketMicros) / coolDownIntervalMicros();
            storedPermits = min(maxPermits, storedPermits + newPermits);
            nextFreeTicketMicros = nowMicros;
        }
    }

    double coolDownIntervalMicros() {
        return stableIntervalMicros;
    }

    private Object mutex() {
        Object mutex = mutexDoNotUseDirectly;
        if (mutex == null) {
            synchronized (this) {
                mutex = mutexDoNotUseDirectly;
                if (mutex == null) {
                    mutex = new Object();
                    mutexDoNotUseDirectly = mutex;
                }
            }
        }
        return mutex;
    }

}
