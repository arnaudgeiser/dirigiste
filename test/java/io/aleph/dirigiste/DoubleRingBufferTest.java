package io.aleph.dirigiste;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class DoubleRingBufferTest {

    @Test
    public void testEmptyBuffer() {
        DoubleRingBuffer ringBuffer = new DoubleRingBuffer(10);
        assertArrayEquals(new double[] {}, ringBuffer.toSortedArray(), 0);
    }

    @Test
    public void testOneElementBuffer() {
        DoubleRingBuffer ringBuffer = new DoubleRingBuffer(10);
        ringBuffer.add(5);
        assertArrayEquals(new double[] {5}, ringBuffer.toSortedArray(), 0);
    }

    @Test
    public void testTenElementsBuffer() {
        DoubleRingBuffer ringBuffer = new DoubleRingBuffer(10);
        reverseList(0, 10).forEach(ringBuffer::add);
        assertArrayEquals(new double[] {0,1,2,3,4,5,6,7,8,9}, ringBuffer.toSortedArray(), 0);
    }

    @Test
    public void testElevenElementsBuffer() {
        DoubleRingBuffer ringBuffer = new DoubleRingBuffer(10);
        IntStream.range(1, 12).forEach(ringBuffer::add);
        assertArrayEquals(new double[] {2,3,4,5,6,7,8,9,10,11}, ringBuffer.toSortedArray(), 0);
    }

    private List<Long> reverseList(int from, int to) {
        return LongStream.range(from, to).map(i -> to - i + from - 1).boxed().collect(toList());
    }
}
