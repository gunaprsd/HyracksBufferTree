package org.apache.hyracks.storage.am.buffertree.common;

import com.google.common.primitives.Shorts;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;


import java.nio.ByteBuffer;

public class KeyValueTuple implements IReusableTupleReference {

    public static final KeyValueTuple HIGHEST_KEY_TUPLE = createHighestTuple();
    public static final KeyValueTuple LOWEST_KEY_TUPLE = createLowestTuple();

    public static final byte PUT = 1;
    public static final byte DELETE = 2;
    public static final byte HIGHEST_KEY = 3;
    public static final byte LOWEST_KEY = 4;
    protected byte[] data;
    protected int offset;
    protected int length;
    protected ByteBuffer wrapper;

    //Helper function
    protected short getShort(byte[] data, int off) {
        return Shorts.fromBytes(data[off], data[off+1]) ;
    }

    //Constructors
    public KeyValueTuple() {
        reset();
    }

    public KeyValueTuple(ITupleReference tuple) {
        reset(tuple);
    }

    public KeyValueTuple(byte[] data, int offset, int length) {
        reset(data, offset, length);
    }

    public KeyValueTuple(ByteBuffer buffer, int off) {
        reset(buffer, off);
    }

    //Reset functions for reusable tuples
    public void reset() {
        data = null;
        offset = -1;
        length = -1;
        wrapper = null;
    }

    public void reset(ITupleReference tuple) {
        if(!KeyValueTuple.class.isAssignableFrom(tuple.getClass())) {
            throw new AssertionError();
        }
        data = ((KeyValueTuple)tuple).getArray();
        offset = ((KeyValueTuple)tuple).getOffset();
        length = ((KeyValueTuple)tuple).getLength();
        wrapper = ByteBuffer.wrap(data, offset, length);
    }

    public void reset(ByteBuffer buffer, int off) {
        this.data = buffer.array();
        this.offset = off;
        this.length = 1 + 2 + 2 + buffer.getShort(off + 1) + buffer.getShort(off + 3);
        this.wrapper = ByteBuffer.wrap(data, offset, length);
    }

    public void reset(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.wrapper = ByteBuffer.wrap(data, offset, length);
    }

    //Accessor functions for the class fields
    public boolean isNull() {
        return data == null;
    }

    public byte getType() {
        return wrapper.get(offset);
    }

    public byte[] getArray() {
        return data;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public static KeyValueTuple createHighestTuple() {
        ByteBuffer buf = ByteBuffer.allocate(5);
        buf.put(0, HIGHEST_KEY);
        buf.putShort(1, (short)0);
        buf.putShort(3, (short)0);

        return new KeyValueTuple(buf.array(), 0, 5);
    }

    public static KeyValueTuple createLowestTuple() {
        ByteBuffer buf = ByteBuffer.allocate(5);
        buf.put(0, LOWEST_KEY);
        buf.putShort(1, (short)0);
        buf.putShort(3, (short)0);

        return new KeyValueTuple(buf.array(), 0, 5);
    }

    public void deepCopy(ITupleReference tuple) {
        if(!KeyValueTuple.class.isAssignableFrom(tuple.getClass())) {
            throw new AssertionError();
        }

        this.length = ((KeyValueTuple)tuple).getLength();
        this.data = new byte[length];
        this.offset = 0;
        System.arraycopy(((KeyValueTuple) tuple).getArray(), ((KeyValueTuple) tuple).getOffset(), data, offset, length);
        this.wrapper = ByteBuffer.wrap(data, offset, length);
    }

    public int getFieldCount() {
        return 3;
    }

    public byte[] getFieldData(int i) {
        return data;
    }

    public int getFieldStart(int i) {
        if(i == 0) {
            return offset;
        } else if(i == 1) {
            return offset + 5;
        } else if(i == 2) {
            return offset + 5 + getShort(data, offset + 1);
        } else {
            throw new AssertionError("KeyValueTuple index must be <= 2");
        }
    }

    public int getFieldLength(int i) {
        if(i == 0) {
            return 1;
        } else if(i == 1) {
            return getShort(data, offset+1);
        } else if(i == 2) {
            return getShort(data, offset+3);
        } else {
            throw new AssertionError("KeyValueTuple index must be <= 2");
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        byte type = getType();
        if(type == HIGHEST_KEY) {
            sb.append("HIGHEST");
        } else if(type == LOWEST_KEY) {
            sb.append("LOWEST");
        } else if(type == PUT) {
            long key = wrapper.getLong(offset + 5);
            sb.append("PUT");
            sb.append("/");
            sb.append(key);
            sb.append("/len=");
            sb.append(getLength());
        } else {
            long key = wrapper.getLong(offset + 5);
            sb.append("DELETE");
            sb.append("/");
            sb.append(key);
            sb.append("/len=");
            sb.append(getLength());
        }

        return sb.toString();
    }

    public byte[] getKey() {
        byte[] key = new byte[getFieldLength(1)];
        System.arraycopy(data, offset + 5, key, 0, key.length);
        return key;
    }
}
