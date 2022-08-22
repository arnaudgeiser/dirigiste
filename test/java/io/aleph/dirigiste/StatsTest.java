package io.aleph.dirigiste;

import io.aleph.dirigiste.Stats.LongRingBufferMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;

public class StatsTest {

    @Test
    public void testLongRingBufferMap() {
        LongRingBufferMap<Key> longRingBufferMap = new LongRingBufferMap<>(30);
        IntStream.range(0, 20).forEach(i -> longRingBufferMap.sample(new Key(UUID.randomUUID().toString()), ThreadLocalRandom.current().nextInt(100)));
        Map<Key, LongRingBuffer> ringBuffers = longRingBufferMap._ringBuffers;
        assertEquals(20, ringBuffers.size());
        assertEquals(20, longRingBufferMap.toMap().size());
        assertEquals(20, ringBuffers.size());
        ringBuffers.keySet().forEach(k -> assertSame(ringBuffers.get(k), longRingBufferMap._ringBuffers.get(k)));
        longRingBufferMap._ringBuffers.keySet().forEach(longRingBufferMap::remove);
        assertTrue(longRingBufferMap._ringBuffers.isEmpty());
    }

    @Test
    public void testDoubleRingBufferMap() {
        Stats.DoubleRingBufferMap<Key> doubleRingBufferMap = new Stats.DoubleRingBufferMap<>(30);
        IntStream.range(0, 20).forEach(i -> doubleRingBufferMap.sample(new Key(UUID.randomUUID().toString()), ThreadLocalRandom.current().nextInt(100)));
        Map<Key, DoubleRingBuffer> ringBuffers = doubleRingBufferMap._ringBuffers;
        assertEquals(20, ringBuffers.size());
        doubleRingBufferMap.remove(doubleRingBufferMap._ringBuffers.keySet().iterator().next());
        assertEquals(19, doubleRingBufferMap.toMap().size());
        assertEquals(19, ringBuffers.size());
    }

    @Test
    public void testGetMetrics() {
        Stats stats = new Stats(EnumSet.of(Stats.Metric.UTILIZATION), 5, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(EnumSet.of(Stats.Metric.UTILIZATION), stats.getMetrics());
    }

    @Test
    public void testGetNumWorkers() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 5, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(5, stats.getNumWorkers());
    }

    @Test
    public void testGetUtilizationWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getUtilization(1), 0);
    }

    @Test
    public void testGetUtilizationWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, null, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithNullStat(stats::getUtilization);
    }

    @Test
    public void testGetUtilizationWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {5}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithSingleStat(stats::getUtilization);
    }

    @Test
    public void testGetUtilizationWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {0, 0, 1, 2, 4, 5}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithSomeStats(stats::getUtilization);
    }

    @Test
    public void testGetMeanUtilizationWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getMeanUtilization(), 0);
    }

    @Test
    public void testGetMeanUtilizationWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, null, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(0, stats.getMeanUtilization(), 0);
    }

    @Test
    public void testGetMeanUtilizationWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {5}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(5, stats.getMeanUtilization(), 0);
    }

    @Test
    public void testGetMeanUtilizationWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {0, 0, 1, 2, 4, 5}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(2, stats.getMeanUtilization(), 0);
    }

    @Test
    public void testGetTaskArrivalRateWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getTaskArrivalRate(1), 0);
    }

    @Test
    public void testGetTaskArrivalRateWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, null, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithNullStat(stats::getTaskArrivalRate);
    }

    @Test
    public void testGetTaskArrivalRateWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {5}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithSingleStat(stats::getTaskArrivalRate);
    }

    @Test
    public void testGetMeanTaskArrivalRateWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getMeanTaskArrivalRate(), 0);
    }

    @Test
    public void testGetMeanTaskArrivalRateWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, null, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(0, stats.getMeanTaskArrivalRate(), 0);
    }

    @Test
    public void testGetMeanTaskArrivalRateWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {5}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(5, stats.getMeanTaskArrivalRate(), 0);
    }

    @Test
    public void testGetMeanTaskArrivalRateWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {0, 0, 1, 2, 4, 5}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(2, stats.getMeanTaskArrivalRate(), 0);
    }

    @Test
    public void testGetTaskArrivalRateWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {0, 0, 1, 2, 4, 5}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithSomeStats(stats::getTaskArrivalRate);
    }

    @Test
    public void testGetTaskCompletionRateWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getTaskCompletionRate(1), 0);
    }

    @Test
    public void testGetTaskCompletionRateWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, null,
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithNullStat(stats::getTaskCompletionRate);
    }

    @Test
    public void testGetTaskCompletionRateWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {5},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithSingleStat(stats::getTaskCompletionRate);
    }

    @Test
    public void testGetTaskCompletionRateWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {0, 0, 1, 2, 4, 5},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithSomeStats(stats::getTaskCompletionRate);
    }

    @Test
    public void testGetMeanTaskCompletionRateWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getMeanTaskCompletionRate(), 0);
    }

    @Test
    public void testGetMeanTaskCompletionRateWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, null,
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(0, stats.getMeanTaskCompletionRate(), 0);
    }

    @Test
    public void testGetMeanTaskCompletionRateWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {5},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(5, stats.getMeanTaskCompletionRate(), 0);
    }

    @Test
    public void testGetMeanTaskCompletionRateWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {0, 0, 1, 2, 4, 5},
                new double[] {}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(2, stats.getMeanTaskCompletionRate(), 0);
    }

    @Test
    public void testGetTaskRejectionRateWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getTaskRejectionRate(1), 0);
    }

    @Test
    public void testGetTaskRejectionRateWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                null, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithNullStat(stats::getTaskRejectionRate);
    }

    @Test
    public void testGetTaskRejectionRateWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {5}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithSingleStat(stats::getTaskRejectionRate);
    }

    @Test
    public void testGetTaskRejectionRateWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {0, 0, 1, 2, 4, 5}, new long[] {}, new long[] {}, new long[] {});
        lerpBehaviorWithSomeStats(stats::getTaskRejectionRate);
    }

    @Test
    public void testGetMeanTaskRejectionRateWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getMeanTaskRejectionRate(), 0);
    }

    @Test
    public void testGetMeanTaskRejectionRateWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                null, new long[] {}, new long[] {}, new long[] {});
        assertEquals(0, stats.getMeanTaskRejectionRate(), 0);
    }

    @Test
    public void testGetMeanTaskRejectionRateWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {5}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(5, stats.getMeanTaskRejectionRate(), 0);
    }

    @Test
    public void testGetMeanTaskRejectionRateWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {0, 0, 1, 2, 4, 5}, new long[] {}, new long[] {}, new long[] {});
        assertEquals(2, stats.getMeanTaskRejectionRate(), 0);
    }

    @Test
    public void testGetQueueLengthWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getQueueLength(1), 0);
    }

    @Test
    public void testGetQueueLengthWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, null, new long[] {}, new long[] {});
        lerpBehaviorWithNullStat(stats::getQueueLength);
    }

    @Test
    public void testGetQueueLengthWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {5}, new long[] {}, new long[] {});
        lerpBehaviorWithSingleStat(stats::getQueueLength);
    }

    @Test
    public void testGetQueueLengthWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {0, 0, 1, 2, 4, 5}, new long[] {}, new long[] {});
        lerpBehaviorWithSomeStats(stats::getQueueLength);
    }

    @Test
    public void testGetMeanQueueLengthWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getMeanQueueLength(), 0);
    }

    @Test
    public void testGetMeanQueueLengthWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, null, new long[] {}, new long[] {});
        assertEquals(0, stats.getMeanQueueLength(), 0);
    }

    @Test
    public void testGetMeanQueueLengthWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {5}, new long[] {}, new long[] {});
        assertEquals(5, stats.getMeanQueueLength(), 0);
    }

    @Test
    public void testGetMeanQueueLengthWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {0, 0, 1, 2, 4, 5}, new long[] {}, new long[] {});
        assertEquals(2, stats.getMeanQueueLength(), 0);
    }

    @Test
    public void testGetQueueLatencyWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getQueueLatency(1), 0);
    }

    @Test
    public void testGetQueueLatencyWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, null, new long[] {});
        lerpBehaviorWithNullStat(stats::getQueueLatency);
    }

    @Test
    public void testGetQueueLatencyWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {5}, new long[] {});
        lerpBehaviorWithSingleStat(stats::getQueueLatency);
    }

    @Test
    public void testGetQueueLatencyWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {0, 0, 1, 2, 4, 5}, new long[] {});
        lerpBehaviorWithSomeStats(stats::getQueueLatency);
    }

    @Test
    public void testGetMeanQueueLatencyWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getMeanQueueLatency(), 0);
    }

    @Test
    public void testGetMeanQueueLatencyWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, null, new long[] {});
        assertEquals(0, stats.getMeanQueueLatency(), 0);
    }

    @Test
    public void testGetMeanQueueLatencyWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {5}, new long[] {});
        assertEquals(5, stats.getMeanQueueLatency(), 0);
    }

    @Test
    public void testGetMeanQueueLatencyWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {0, 0, 1, 2, 4, 5}, new long[] {});
        assertEquals(2, stats.getMeanQueueLatency(), 0);
    }

    @Test
    public void testGetTaskLatencyWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getQueueLatency(1), 0);
    }

    @Test
    public void testGetTaskLatencyWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, null);
        lerpBehaviorWithNullStat(stats::getTaskLatency);
    }

    @Test
    public void testGetTaskLatencyWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {5});
        lerpBehaviorWithSingleStat(stats::getTaskLatency);
    }

    @Test
    public void testGetTaskLatencyWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {0, 0, 1, 2, 4, 5});
        lerpBehaviorWithSomeStats(stats::getTaskLatency);
    }

    @Test
    public void testGetMeanTaskLatencyWithEmptyStats() {
        assertEquals(0, Stats.EMPTY.getQueueLatency(1), 0);
    }

    @Test
    public void testGetMeanTaskLatencyWithNullStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, null);
        assertEquals(0, stats.getMeanTaskLatency(), 0);
    }

    @Test
    public void testGetMeanTaskLatencyWithSingleStat() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {5});
        assertEquals(5, stats.getMeanTaskLatency(), 0);
    }

    @Test
    public void testGetMeanTaskLatencyWithSomeStats() {
        Stats stats = new Stats(EnumSet.allOf(Stats.Metric.class), 0, new double[] {}, new double[] {}, new double[] {},
                new double[] {}, new long[] {}, new long[] {}, new long[] {0, 0, 1, 2, 4, 5});
        assertEquals(2, stats.getMeanTaskLatency(), 0);
    }

    private void lerpBehaviorWithNullStat(Function<Double, Double> f) {
        assertEquals(0, f.apply(1.0), 0);
        assertEquals(0, f.apply(0.75), 0);
        assertEquals(0, f.apply(0.5), 0);
        assertEquals(0, f.apply(0.25), 0);
        assertEquals(0, f.apply(0.0), 0);
        assertEquals(0, f.apply(-1.0), 0);
        assertEquals(0, f.apply(2.0), 0);
    }

    private void lerpBehaviorWithSingleStat(Function<Double, Double> f) {
        assertEquals(5, f.apply(1.0), 0);
        assertEquals(5, f.apply(0.75), 0);
        assertEquals(5, f.apply(0.5), 0);
        assertEquals(5, f.apply(0.25), 0);
        assertEquals(5, f.apply(0.0), 0);
        assertThrows(IllegalArgumentException.class, () -> f.apply(-1.0));
        assertThrows(IllegalArgumentException.class, () -> f.apply(2.0));
    }

    private void lerpBehaviorWithSomeStats(Function<Double, Double> f) {
        assertEquals(5, f.apply(1.0), 0);
        assertEquals(3.5, f.apply(0.75), 0);
        assertEquals(1.5, f.apply(0.5), 0);
        assertEquals(0.25, f.apply(0.25), 0);
        assertEquals(0, f.apply(0.0), 0);
        assertThrows(IllegalArgumentException.class, () -> f.apply(-1.0));
        assertThrows(IllegalArgumentException.class, () -> f.apply(2.0));
    }

    // An identity-based key
    private static class Key {
        public final String value;

        Key(String value) {
            this.value = value;
        }
    }
}
