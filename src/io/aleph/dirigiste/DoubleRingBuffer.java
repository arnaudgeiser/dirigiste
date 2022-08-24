package io.aleph.dirigiste;

import java.util.Arrays;

public class DoubleRingBuffer {

    volatile double[] _values;
    private final int _size;
    private volatile int _count;
    private volatile int _offset;

    public DoubleRingBuffer(final int size) {
        _values = new double[size];
        _size = size;
    }

    public synchronized void add(final double value) {
        _values[_offset++] = value;
        _count = Math.min(_count + 1, _size);
        _offset %= _size;
    }

    public double[] toSortedArray() {
        double[] vals = Arrays.copyOf(_values, _count);
        Arrays.sort(vals);
        return vals;
    }
}
