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
        _count++;
        if(_offset>=_size) {
            _offset = 0;
        }
    }

    public long[] toArray() {
        int cnt = Math.min(_count, _size);
        long[] vals = Arrays.copyOf(_values, cnt);
        Arrays.sort(vals);
        return vals;
    }

    public double[] toDoubleArray() {
        int cnt = Math.min(_count, _size);
        double[] vals = new double[cnt];
        for (int i = 0; i < cnt; i++) {
            vals[i] = Double.longBitsToDouble(_values[i]);
        }
        Arrays.sort(vals);
        return vals;
    }
}
