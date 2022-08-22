package io.aleph.dirigiste;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.*;
import java.util.stream.Collectors;

public class Stats {

    public enum Metric {
        QUEUE_LENGTH,
        QUEUE_LATENCY,
        TASK_LATENCY,
        TASK_ARRIVAL_RATE,
        TASK_COMPLETION_RATE,
        TASK_REJECTION_RATE,
        UTILIZATION
    }

    static final int RESERVOIR_SIZE = 4096;

    public static class UniformLongReservoir {

        final LongRingBuffer _ringBuffer;

        UniformLongReservoir() {
            this(RESERVOIR_SIZE);
        }

        UniformLongReservoir(int reservoirSize) {
            this._ringBuffer = new LongRingBuffer(reservoirSize);
        }

        public void sample(long n) {
            _ringBuffer.add(n);
        }

        public long[] toArray() {
            return _ringBuffer.toSortedArray();
        }
    }

    public static class UniformDoubleReservoir {

        final LongRingBuffer _ringBuffer;

        UniformDoubleReservoir() {
            this(RESERVOIR_SIZE);
        }

        UniformDoubleReservoir(int reservoirSize) {
            this._ringBuffer = new LongRingBuffer(reservoirSize);
        }

        public void sample(double n) {
            _ringBuffer.add(Double.doubleToLongBits(n));
        }

        public double[] toArray() {
            return _ringBuffer.toDoubleArray();
        }
    }

    public static class UniformLongReservoirMap<K> {
        ConcurrentHashMap<K,UniformLongReservoir> _reservoirs =
                new ConcurrentHashMap<>();
        private final int _reservoirSize;

        public UniformLongReservoirMap() {
            this(RESERVOIR_SIZE);
        }

        public UniformLongReservoirMap(int reservoirSize) {
            _reservoirSize = Math.min(reservoirSize, RESERVOIR_SIZE);
        }

        public void sample(K key, long n) {
            UniformLongReservoir r = _reservoirs.get(key);
            if (r == null) {
                r = new UniformLongReservoir(_reservoirSize);
                UniformLongReservoir prior = _reservoirs.putIfAbsent(key, r);
                r = (prior == null ? r : prior);
            }
            r.sample(n);
        }

        public Map<K,long[]> toMap() {
            return _reservoirs.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray()));
        }

        public void remove(K key) {
            _reservoirs.remove(key);
        }
    }

    public static class UniformDoubleReservoirMap<K> {
        ConcurrentHashMap<K,UniformDoubleReservoir> _reservoirs =
                new ConcurrentHashMap<>();
        private final int _reservoirSize;

        public UniformDoubleReservoirMap() {
            this(RESERVOIR_SIZE);
        }

        public UniformDoubleReservoirMap(int reservoirSize) {
            this._reservoirSize = Math.min(reservoirSize, RESERVOIR_SIZE);
        }

        public void sample(K key, double n) {
            UniformDoubleReservoir r = _reservoirs.get(key);
            if (r == null) {
                r = new UniformDoubleReservoir(_reservoirSize);
                UniformDoubleReservoir prior = _reservoirs.putIfAbsent(key, r);
                r = (prior == null ? r : prior);
            }
            r.sample(n);
        }

        public Map<K,double[]> toMap() {
            return _reservoirs.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray()));
        }

        public void remove(K key) {
            _reservoirs.remove(key);
        }
    }

    public static double lerp(long low, long high, double t) {
        return low + (high - low) * t;
    }

    public static double lerp(double low, double high, double t) {
        return low + (high - low) * t;
    }

    public static double lerp(long[] vals, double t) {

        if (vals == null) {
            return 0;
        }

        if (t < 0 || 1 < t) {
            throw new IllegalArgumentException(Double.toString(t));
        }

        int cnt = vals.length;

        switch (cnt) {
        case 0:
            return 0.0;
        case 1:
            return (double) vals[0];
        default:
            if (t == 1.0) {
                return (double) vals[cnt-1];
            }
            double idx = (cnt-1) * t;
            int iidx = (int) idx;
            return lerp(vals[iidx], vals[iidx + 1], idx - iidx);
        }
    }

    public static double lerp(double[] vals, double t) {

        if (vals == null) {
            return 0;
        }

        if (t < 0 || 1 < t) {
            throw new IllegalArgumentException(Double.toString(t));
        }

        int cnt = vals.length;

        switch (cnt) {
        case 0:
            return 0.0;
        case 1:
            return vals[0];
        default:
            if (t == 1.0) {
                return vals[cnt-1];
            }
            double idx = (cnt-1) * t;
            int iidx = (int) idx;
            return lerp(vals[iidx], vals[iidx + 1], idx - iidx);
        }
    }

    public static double mean(double[] vals) {
        if (vals == null || vals.length == 0) {
            return 0;
        }

        double sum = 0;
        for (double val : vals) {
            sum += val;
        }
        return sum/vals.length;
    }

    public static double mean(long[] vals) {
        if (vals == null || vals.length == 0) {
            return 0;
        }

        long sum = 0;
        for (long val : vals) {
            sum += val;
        }
        return sum/vals.length;
    }

    //

    private final EnumSet<Metric> _metrics;

    private final int _numWorkers;
    private final double[] _utilizations;
    private final double[] _taskArrivalRates;
    private final double[] _taskCompletionRates;
    private final double[] _taskRejectionRates;
    private final long[] _queueLengths;
    private final long[] _queueLatencies;
    private final long[] _taskLatencies;

    public static final Stats EMPTY = new Stats(EnumSet.noneOf(Metric.class), 0, new double[] {}, new double[] {}, new double[] {}, new double[] {}, new long[] {}, new long[] {}, new long[] {});

    public Stats(EnumSet<Metric> metrics, int numWorkers, double[] utilizations, double[] taskArrivalRates, double[] taskCompletionRates, double[] taskRejectionRates, long[] queueLengths, long[] queueLatencies, long[] taskLatencies) {
        _metrics = metrics;
        _numWorkers = numWorkers;
        _utilizations = utilizations;
        _taskArrivalRates = taskArrivalRates;
        _taskCompletionRates = taskCompletionRates;
        _taskRejectionRates = taskRejectionRates;
        _queueLengths = queueLengths;
        _queueLatencies = queueLatencies;
        _taskLatencies = taskLatencies;
    }

    /**
     * @return the provided metrics
     */
    public EnumSet<Metric> getMetrics() {
        return _metrics;
    }

    /**
     * @return the number of active workers in the pool.
     */
    public int getNumWorkers() {
        return _numWorkers;
    }

    /**
     * @return the mean utilization of the workers as a value between 0 and 1.
     */
    public double getMeanUtilization() {
        return mean(_utilizations);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the utilization of the workers as a value between 0 and 1
     */
    public double getUtilization(double quantile) {
        return lerp(_utilizations, quantile);
    }

    /**
     * @return the mean task arrival rate of the executor, in tasks per second
     */
    public double getMeanTaskArrivalRate() {
        return mean(_taskArrivalRates);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the task arrival rate of the executor, in tasks per second
     */
    public double getTaskArrivalRate(double quantile) {
        return lerp(_taskArrivalRates, quantile);
    }

    /**
     * @return the mean task completion rate of the executor, in tasks per second
     */
    public double getMeanTaskCompletionRate() {
        return mean(_taskCompletionRates);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the task completion rate of the executor, in tasks per second
     */
    public double getTaskCompletionRate(double quantile) {
        return lerp(_taskCompletionRates, quantile);
    }

    /**
     * @return the mean task rejection rate of the executor, in tasks per second
     */
    public double getMeanTaskRejectionRate() {
        return mean(_taskRejectionRates);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the task rejection rate of the executor, in tasks per second
     */
    public double getTaskRejectionRate(double quantile) {
        return lerp(_taskRejectionRates, quantile);
    }

    /**
     * @return the mean length of the queue
     */
    public double getMeanQueueLength() {
        return mean(_queueLengths);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the length of the queue
     */
    public double getQueueLength(double quantile) {
        return lerp(_queueLengths, quantile);
    }

    /**
     * @return the mean time each task spends on the queue, in nanoseconds
     */
    public double getMeanQueueLatency() {
        return mean(_queueLatencies);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the time each task spends on the queue, in nanoseconds
     */
    public double getQueueLatency(double quantile) {
        return lerp(_queueLatencies, quantile);
    }

    /**
     * @return the mean time each task takes to complete, including time on the queue, in nanoseconds
     */
    public double getMeanTaskLatency() {
        return mean(_taskLatencies);
    }

    /**
     * @param quantile  the point within the distribution to look up, 0.5 returns the median, 0.9 the 90th percentile
     * @return the time each task takes to complete, including time on the queue, in nanoseconds
     */
    public double getTaskLatency(double quantile) {
        return lerp(_taskLatencies, quantile);
    }
}
