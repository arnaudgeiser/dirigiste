package io.aleph.dirigiste;

import java.util.Arrays;

public class LongRingBuffer {

    final long[] _values;
    private final int _size;
    private int _count;
    private int _offset;

    public LongRingBuffer(final int size) {
        _values = new long[size];
        _size = size;
    }

    public void add(final long value) {
        _values[_offset++] = value;
        _count = Math.min(_count + 1, _size);
        _offset %= _size;
    }

    public long[] toSortedArray() {
        long[] vals = Arrays.copyOf(_values, _count);
        Arrays.sort(vals);
        return vals;
    }
}
