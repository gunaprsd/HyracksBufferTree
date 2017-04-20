package org.apache.hyracks.storage.am.buffertree.common;

import com.google.common.hash.BloomFilter;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.IBloomFilterFactory;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.ITupleWriter;

import java.nio.ByteBuffer;

public class KeyValueTupleWriter implements ITupleWriter {

    public static final KeyValueTupleWriter INSTANCE = new KeyValueTupleWriter(BloomFilterFactory.INSTANCE);

    public final IBloomFilterFactory bloomFilterFactory;

    public KeyValueTupleWriter(IBloomFilterFactory bloomFilterFactory) {
        this.bloomFilterFactory = bloomFilterFactory;
    }

    public int writeTuple(ITupleReference tuple, ByteBuffer targetBuf, int targetOff) {
        if(!KeyValueTuple.class.isAssignableFrom(tuple.getClass())) {
            throw new AssertionError();
        }
        KeyValueTuple kvTuple = (KeyValueTuple)tuple;
        targetBuf.position(targetOff);
        targetBuf.put(kvTuple.getArray(), kvTuple.getOffset(), kvTuple.getLength());
        return 0;
    }

    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        KeyValueTuple kvTuple = (KeyValueTuple)tuple;
        System.arraycopy(kvTuple.getArray(), kvTuple.getOffset(), targetBuf, targetOff, kvTuple.getLength());
        return 0;
    }

    public int bytesRequired(ITupleReference tuple) {
        return ((KeyValueTuple)tuple).getLength();
    }

    public int writeTupleFields(ITupleReference tuple, int startField, int numFields, byte[] targetBuf, int targetOff) {
        if (false) throw new AssertionError();
        return -1;
    }

    public int bytesRequired(ITupleReference tuple, int startField, int numFields) {
        if (false) throw new AssertionError();
        return -1;
    }

    public IReusableTupleReference createNewTuple() {
        return new KeyValueTuple();
    }

    public IReusableTupleReference createNewTuple(ITupleReference tuple) {
        return new KeyValueTuple(tuple);
    }

    public IIndexTupleReference createNewIndexTuple() {
        return new KeyValueIndexTuple();
    }

    public IIndexTupleReference createNewIndexTuple(IIndexTupleReference tuple) {
        KeyValueIndexTuple indexTuple = new KeyValueIndexTuple(tuple);
        return indexTuple;
    }

    public IIndexTupleWithBloomFilter createNewIndexTupleWithBF() {
        BloomFilter bf = bloomFilterFactory.createEmptyBloomFilter();
        int bfSize = bloomFilterFactory.getSize();
        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + 2 + 4 + bfSize);
        buf.put(0, KeyValueTuple.PUT);
        buf.putShort(1, (short)0);
        buf.putShort(3, (short)(bfSize + 4));
        KeyValueIndexTupleWithBloomFilter.writeBloomFilter(bf, buf, 5 + 4);
        return new KeyValueIndexTupleWithBloomFilter(buf, 0);
    }


}
