package org.apache.hyracks.storage.am.buffertree.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeIndexFrame;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueComparator;
import org.apache.hyracks.storage.am.buffertree.impl.FrameOpStatus;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public abstract  class AbstractBufferTreeIndexFrame implements ITreeIndexFrame {

    static public final byte leafType = (byte)1;
    static public final byte leafNodeType = (byte)2;
    static public final byte interiorNodeType = (byte)3;

    static public final int typeOff = 0;
    static public final int levelOff = typeOff + 1;
    static public final int smFlagOff = levelOff + 1;
    static public final int pageLsnOff = smFlagOff + 1;
    static public final int prevSiblingIdOff = pageLsnOff + 8;
    static public final int nextSiblingIdOff = prevSiblingIdOff + 4;
    static public final int pageHeaderSize = nextSiblingIdOff + 4;

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;

    protected IReusableTupleReference frameTuple;
    protected IIndexTupleReference frameIndexTuple = null;

    public void setType(byte type) {
        buf.put(typeOff, type);
    }

    public boolean isLeaf() {
        return buf.get(typeOff) == leafType;
    }

    public boolean isNode() {
        return buf.get(typeOff) == interiorNodeType || buf.get(typeOff) == leafNodeType;
    }

    public boolean isLeafNode() {
        return buf.get(typeOff) == leafNodeType;
    }

    public int getPageHeaderSize() {
        return pageHeaderSize;
    }

    public int getPreviousSiblingId() {
        return buf.getInt(prevSiblingIdOff);
    }

    public int getNextSiblingId() {
        return buf.getInt(nextSiblingIdOff);
    }

    public long getPageLsn() {
        return buf.getLong(pageLsnOff);
    }

    public boolean getSmFlag() {
        return buf.get(smFlagOff) != (byte)0;
    }

    public byte getLevel() {
        return buf.get(levelOff);
    }

    public void setPreviousSiblingId(int pageId) {
        buf.putInt(prevSiblingIdOff, pageId);
    }

    public void setNextSiblingId(int pageId) {
        buf.putInt(nextSiblingIdOff, pageId);
    }

    public void setPageLsn(long pageLsn) {
        buf.putLong(pageLsnOff, pageLsn);
    }

    public void setSmFlag(boolean smFlag) {
        if(smFlag)
            buf.put(smFlagOff, (byte)1);
        else
            buf.put(smFlagOff, (byte)0);
    }

    public void setLevel(byte level) {
        buf.put(levelOff, level);
    }

    public ICachedPage getPage() {
        return page;
    }

    public void setPage(ICachedPage page) {
        this.page = page;
        this.buf = page.getBuffer();
    }

    public ByteBuffer getBuffer() {
        return buf;
    }

    public void split(int splitSize, ITupleIterator elementsIterator, IBulkTupleInserter leftBin, IBulkTupleInserter rightBin, IIndexTupleReference splitKey) throws HyracksDataException {
        IBulkTupleInserter current;

        int size = 0;
        current = leftBin;
        leftBin.init();
        while(elementsIterator.hasNext()) {
            frameTuple.reset(elementsIterator.peek());

            if(size > splitSize) {
                leftBin.end();
                current = rightBin;
                rightBin.init();
                splitKey.setKey(frameTuple);
                size = 0;
            }

            size += current.getBytesRequiredToWriteTuple(frameTuple);
            FrameOpStatus opStatus = current.insert(frameTuple);
            assert opStatus == FrameOpStatus.COMPLETED;
            elementsIterator.next();
        }
        rightBin.end();
    }

    public void split(int splitSize, ITupleIterator elementsIterator, ArrayList<? extends IBulkTupleInserter> bins, ArrayList<? extends IIndexTupleReference> splitKeys) throws HyracksDataException {
        if (bins.size() != splitKeys.size() + 1) throw new AssertionError();
        IBulkTupleInserter current;

        int index = 0;
        int size = 0, length = 0;
        current = bins.get(index);
        current.init();
        while(elementsIterator.hasNext()) {
            frameTuple.reset(elementsIterator.peek());
            length = current.getBytesRequiredToWriteTuple(frameTuple);

            if(size > splitSize) {
                splitKeys.get(index).setKey(frameTuple);
                index++;
                size = 0;
                current.end();
                current = bins.get(index);
                current.init();
            }

            size += length;
            FrameOpStatus opStatus = current.insert(frameTuple);
            if (opStatus != FrameOpStatus.COMPLETED) throw new AssertionError();
            elementsIterator.next();
        }
        if (index != splitKeys.size()) throw new AssertionError();
        current.end();

    }

}
