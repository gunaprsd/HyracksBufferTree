package org.apache.hyracks.storage.am.buffertree.frames;

import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.IPrimaryBufferFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.IPrimaryBufferFrameFactory;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;
import org.apache.hyracks.storage.am.buffertree.impl.SlottedTupleManager;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import java.nio.ByteBuffer;

public class SlottedPrimaryBufferFrame implements IPrimaryBufferFrame {

    public static final int serialIdOff = 0;
    public static final int headerSize = serialIdOff + 4;

    //frame variables
    protected final int size;
    protected final SlottedTupleManager tupleManager;

    //state variables
    protected ICachedPage page;
    protected ByteBuffer buf;

    public SlottedPrimaryBufferFrame(int size, ITupleWriter writer, ITupleComparator comparator) {
        this.size = size;
        this.tupleManager = new SlottedTupleManager(this, headerSize, size, writer, comparator);
    }

    public void initBuffer(int serialId) {
        setSerialId(serialId);
        tupleManager.initParams();
    }

    public void setSerialId(int serialId) {
        buf.putInt(serialIdOff, serialId);
    }

    public int getSerialId() {
        return buf.getInt(serialIdOff);
    }

    public int getTupleCount() {
        return tupleManager.getTupleCount();
    }

    public int getFreeSpace() {
        return tupleManager.getFreeSpace();
    }

    public int getBufferCapacity() {
        return size;
    }

    public void reset() {
        tupleManager.resetParams();
        tupleManager.resetState();
    }

    public FrameOpStatus insert(ITupleIterator iterator) throws HyracksDataException {
        FrameOpStatus status = FrameOpStatus.FAIL;
        while(iterator.hasNext()) {
            if(insert(iterator.peek()) == FrameOpStatus.COMPLETED) {
                status = FrameOpStatus.PARTIALLY_COMPLETED;
                iterator.next();
            } else {
                return status;
            }
        }
        status = FrameOpStatus.COMPLETED;
        return status;
    }

    public FrameOpStatus insert(ITupleReference tuple) throws HyracksDataException {
        return tupleManager.insertTuple(tuple);
    }

    public ITupleIterator getIterator() {
        return tupleManager.getIterator();
    }

    public IOrderedTupleIterator getOrderedIterator() {
        return tupleManager.getOrderedIterator();
    }

    public IBulkTupleInserter getBulkInserter() {
        return tupleManager.getBulkInserter();
    }

    public ITupleReference getMatchingKeyTuple(ITupleReference searchTuple) throws HyracksDataException {
        return tupleManager.getMatchingKeyTuple(searchTuple);
    }

    public ICachedPage getPage() {
        return page;
    }

    public void setPage(ICachedPage page) {
        this.page = page;
        this.buf = page.getBuffer();
        tupleManager.resetState();
    }

    public ByteBuffer getBuffer() {
        return buf;
    }

    public static class Factory implements IPrimaryBufferFrameFactory {
        ITupleWriter writer;
        ITupleComparator comparator;
        int size;

        public Factory(int size, ITupleWriter writer, ITupleComparator comparator) {
            this.size = size;
            this.writer = writer;
            this.comparator = comparator;
        }

        public IPrimaryBufferFrame createFrame() {
            return new SlottedPrimaryBufferFrame(size, writer, comparator);
        }
    }
}
