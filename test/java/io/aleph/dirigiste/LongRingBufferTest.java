package io.aleph.dirigiste;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class LongRingBufferTest {

    @Test
    public void testEmptyBuffer() {
        LongRingBuffer ringBuffer = new LongRingBuffer(10);
        assertArrayEquals(new long[] {}, ringBuffer.toSortedArray());
    }

    @Test
    public void testOneElementBuffer() {
        LongRingBuffer ringBuffer = new LongRingBuffer(10);
        ringBuffer.add(5);
        assertArrayEquals(new long[] {5}, ringBuffer.toSortedArray());
    }

    @Test
    public void testTenElementsBuffer() {
        LongRingBuffer ringBuffer = new LongRingBuffer(10);
        reverseList(0, 10).forEach(ringBuffer::add);
        assertArrayEquals(new long[] {0,1,2,3,4,5,6,7,8,9}, ringBuffer.toSortedArray());
    }

    @Test
    public void testElevenElementsBuffer() {
        LongRingBuffer ringBuffer = new LongRingBuffer(10);
        IntStream.range(1, 12).forEach(ringBuffer::add);
        assertArrayEquals(new long[] {2,3,4,5,6,7,8,9,10,11}, ringBuffer.toSortedArray());
    }

    private List<Long> reverseList(int from, int to) {
        return LongStream.range(from, to).map(i -> to - i + from - 1).boxed().collect(toList());
    }
}
