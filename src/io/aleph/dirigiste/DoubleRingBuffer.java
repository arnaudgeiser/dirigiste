package io.aleph.dirigiste;

import java.util.Arrays;

public class DoubleRingBuffer {

    final double[] _values;
    private final int _size;
    private int _count;
    private int _offset;

    public DoubleRingBuffer(final int size) {
        _values = new double[size];
        _size = size;
    }

    public void add(final double value) {
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
