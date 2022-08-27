package io.aleph.dirigiste;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

public class AtomicLongRingBuffer {

    private final AtomicLongArray _values;
    private final int _size;
    private volatile int _count;
    private volatile int _offset;

    public AtomicLongRingBuffer(final int size) {
        _values = new AtomicLongArray(size);
        _size = size;
    }

    public synchronized void add(final long value) {
        _values.set(_offset++, value);
        _count = Math.min(_count + 1, _size);
        _offset %= _size;
    }

    public long[] toSortedArray() {
        long[] vals = new long[_count];
        for (int i = 0; i < _count; i++) {
            vals[i] = _values.get(i);
        }
        Arrays.sort(vals);
        return vals;
    }
}
