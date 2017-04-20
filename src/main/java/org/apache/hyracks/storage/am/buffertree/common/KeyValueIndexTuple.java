package org.apache.hyracks.storage.am.buffertree.common;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleWithBloomFilter;

import java.nio.ByteBuffer;

public class KeyValueIndexTuple extends KeyValueTuple implements IIndexTupleReference {

    public KeyValueIndexTuple() {
        super();
    }
    public KeyValueIndexTuple(byte[] data, int offset, int length) {
        super(data, offset, length);
    }

    public KeyValueIndexTuple(ByteBuffer buffer, int off) {
        super(buffer, off);
    }

    public KeyValueIndexTuple(IIndexTupleReference tuple) {
        super(tuple);
    }

    public void reset(IIndexTupleReference indexTuple) {
        super.reset(indexTuple);
    }

    public void setKey(ITupleReference key) {
        if(!KeyValueTuple.class.isAssignableFrom(key.getClass())) {
            throw new AssertionError();
        }

        this.length = 1 + 4 + key.getFieldLength(1) + 4;
        this.data = new byte[length];
        this.offset = 0;
        this.wrapper = ByteBuffer.wrap(data, offset, length);
        wrapper.put(offset, ((KeyValueTuple)key).getType());
        wrapper.putShort(offset + 1, (short)key.getFieldLength(1));
        wrapper.putShort(offset + 3, (short)4);
        wrapper.position(5);
        wrapper.put(key.getFieldData(1), key.getFieldStart(1), key.getFieldLength(1));
        if(key.getClass() == KeyValueIndexTuple.class) {
            wrapper.putInt(offset + 5 + key.getFieldLength(1), ((IIndexTupleReference)key).getPageId());
        }
    }

    public int getPageId() {
        return wrapper.getInt(getFieldStart(2));
    }

    public void setPageId(int pageId) {
        wrapper.putInt(getFieldStart(2), pageId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int pageId = getPageId();
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
        } else {
            long key = wrapper.getLong(offset + 5);
            sb.append("DELETE");
            sb.append("/");
            sb.append(key);
        }
        sb.append("/");
        sb.append(pageId);
        sb.append("/");
        sb.append("vLen=" + getFieldLength(2));

        return sb.toString();
    }
}
