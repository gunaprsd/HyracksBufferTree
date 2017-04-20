package org.apache.hyracks.storage.am.buffertree.frames;

import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import java.nio.ByteBuffer;

/**
 * Created by gunaprsd on 29/10/15.
 */
public class TempCachedPage implements ICachedPage {

    protected final int size;
    protected final ByteBuffer buffer;

    public TempCachedPage(int size) {
        this.size = size;
        this.buffer = ByteBuffer.allocate(size);
    }

    public void reset() {
    }

    public void deepCopy(ICachedPage page) {
        if (size != page.getBuffer().capacity()) throw new AssertionError();
        System.arraycopy(page.getBuffer().array(), 0, buffer.array(), 0, size);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void acquireReadLatch() {
        //null
    }

    public void releaseReadLatch() {
        //null
    }

    public void acquireWriteLatch() {
        //null
    }

    public void releaseWriteLatch(boolean markDirty) {
        //null
    }
}
