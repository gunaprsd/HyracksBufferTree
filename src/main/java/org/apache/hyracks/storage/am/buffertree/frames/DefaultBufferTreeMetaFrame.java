package org.apache.hyracks.storage.am.buffertree.frames;

import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeMetaFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeMetaFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import java.nio.ByteBuffer;

public class DefaultBufferTreeMetaFrame implements ITreeMetaFrame {

    protected static final int heightOff = 0;
    protected static final int maxPageOff = heightOff + 4; //4
    protected static final int lastBufferSerialIdOff = maxPageOff + 4; //8
    protected static final int lastEmptiedBufferIdOff = lastBufferSerialIdOff + 4; //12

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;

    public void initBuffer(byte level) {
        if (level != META_PAGE_LEVEL) throw new AssertionError();
        buf.putInt(heightOff, 0);
        buf.putInt(lastBufferSerialIdOff, 0);
        setLastEmptiedBufferSerialId(0);
        setMaxPage(2);
    }

    public int getHeight() {
        return buf.getInt(heightOff);
    }

    public void setHeight(int height) {
        buf.putInt(heightOff, height);
    }

    public void incrementHeight() {
        int height = getHeight();
        setHeight(height + 1);
    }

    public int getNextSerialForBuffer() {
        int next = buf.getInt(lastBufferSerialIdOff) + 1;
        buf.putInt(lastBufferSerialIdOff, next);
        return next;
    }

    public int getLastSerialIdForBuffer() {
        return buf.getInt(lastBufferSerialIdOff);
    }

    public void setLastEmptiedBufferSerialId(int serialId) {
        buf.putInt(lastEmptiedBufferIdOff, serialId);
    }

    public int getLastEmptiedBufferSerialId() {
        return buf.getInt(lastEmptiedBufferIdOff);
    }

    public void setPage(ICachedPage iCachedPage) {
        page = iCachedPage;
        buf = page.getBuffer();
    }

    public ByteBuffer getBuffer() {
        return buf;
    }

    public ICachedPage getPage() {
        return page;
    }

    public byte getLevel() {
        return META_PAGE_LEVEL;
    }

    public void setLevel(byte level) {
        if (level != META_PAGE_LEVEL) throw new AssertionError();
    }

    public int getNextPage() {
        return -1;
    }

    public void setNextPage(int nextPage) {
        if (true) throw new AssertionError();
    }

    public int getMaxPage() {
        return buf.getInt(maxPageOff);
    }

    public void setMaxPage(int maxPage) {
        buf.putInt(maxPageOff, maxPage);
    }

    public int getFreePage() {
        return -1;
    }

    public boolean hasSpace() {
        if (true) throw new AssertionError();
        return false;
    }

    public void addFreePage(int freePage) {
        if (true) throw new AssertionError();
    }

    public boolean isValid() {
        if (true) throw new AssertionError();
        return false;
    }

    public void setValid(boolean b) {
        //never call this
        if (true) throw new AssertionError();
    }

    public int getLSMComponentFilterPageId() {
        if (true) throw new AssertionError();
        return 0;
    }

    public void setLSMComponentFilterPageId(int i) {
        if (true) throw new AssertionError();
    }

    public long getLSN() {
        if (true) throw new AssertionError();
        return 0;
    }

    public void setLSN(long l) {
        if (true) throw new AssertionError();
    }

    public static class Factory implements ITreeIndexMetaDataFrameFactory, ITreeMetaFrameFactory {

        public Factory() {}

        public ITreeMetaFrame createFrame() {
            return new DefaultBufferTreeMetaFrame();
        }
    }

}
