package org.apache.hyracks.storage.am.buffertree.common;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleWithBloomFilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class KeyValueIndexTupleWithBloomFilter extends KeyValueIndexTuple implements IIndexTupleWithBloomFilter {

    public static final ByteArrayOutputStream out = new ByteArrayOutputStream();

    public KeyValueIndexTupleWithBloomFilter() {
        super();
    }

    public KeyValueIndexTupleWithBloomFilter(IIndexTupleWithBloomFilter tuple) {
        super(tuple);
    }

    public KeyValueIndexTupleWithBloomFilter(ByteBuffer buf, int offset) {
        super(buf, offset);
    }

    public KeyValueIndexTupleWithBloomFilter(byte[] data, int offset, int length) {
        super(data, offset, length);
    }

    @Override
    public void setKey(ITupleReference key) {
        int bfSize = BloomFilterFactory.INSTANCE.getSize();
        int keyLength = key.getFieldLength(1);

        int length = 1 + 4 + keyLength + 4 + bfSize;
        byte[] data = new byte[length];
        int offset = 0;
        ByteBuffer wrapper = ByteBuffer.wrap(data, offset, length);
        wrapper.put(offset, ((KeyValueTuple)key).getType());
        wrapper.putShort(offset + 1, (short)keyLength);
        wrapper.putShort(offset + 3, (short)(4 + bfSize));
        wrapper.position(5);
        wrapper.put(key.getFieldData(1), key.getFieldStart(1), key.getFieldLength(1));
        if(key.getClass() == KeyValueIndexTuple.class) {
            wrapper.putInt(getFieldStart(2), ((IIndexTupleReference)key).getPageId());
        }
        wrapper.position(1 + 4 + keyLength + 4);
        wrapper.put(this.data, getFieldStart(2) + 4, bfSize);

        this.data = data;
        this.offset = offset;
        this.length = length;
        this.wrapper = wrapper;
    }

    public void setBloomFilter(BloomFilter bf) {
        int bfOffset = getFieldStart(2) + 4;
        writeBloomFilter(bf, data, bfOffset);
    }

    public void addBloomFilter(BloomFilter thatBf) {
        if (thatBf == null) throw new AssertionError();
        int bfOffset = getFieldStart(2) + 4;
        BloomFilter thisBf = getBloomFilter();
        if (!thisBf.isCompatible(thatBf)) throw new AssertionError();
        thisBf.putAll(thatBf);
        writeBloomFilter(thisBf, data, bfOffset);
    }

    public BloomFilter getBloomFilter() {
        int bfOffset = getFieldStart(2) + 4;
        int bfSize = BloomFilterFactory.INSTANCE.getSize();
        ByteArrayInputStream in = new ByteArrayInputStream(data, bfOffset, bfSize);
        BloomFilter thisBf = null;
        try {
            thisBf = BloomFilter.readFrom(in, Funnels.byteArrayFunnel());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return thisBf;
    }

    public static void writeBloomFilter(BloomFilter bf, byte[] data, int off) {
        out.reset();
        try {
            bf.writeTo(out);
            System.arraycopy(out.toByteArray(), 0, data, off, out.size());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void writeBloomFilter(BloomFilter bf, ByteBuffer buf, int off) {
        out.reset();
        try {
            bf.writeTo(out);
            buf.position(off);
            buf.put(out.toByteArray(), 0, out.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            sb.append("PUT");
            sb.append("/");
            if(getFieldLength(1) == 8) {
                sb.append(wrapper.getLong(getFieldStart(1)));
            } else {
                sb.append("keyLen=" + getFieldLength(1));
            }
        } else {
            sb.append("DELETE");
            sb.append("/");
            if(getFieldLength(1) == 8) {
                sb.append(wrapper.getLong(getFieldStart(1)));
            } else {
                sb.append("keyLen=" + getFieldLength(1));
            }
        }
        sb.append("/pageId=");
        sb.append(pageId);
        sb.append("/");
        sb.append("vLen=" + getFieldLength(2));

        return sb.toString();
    }
}
