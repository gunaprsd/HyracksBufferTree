package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.IOrderedTupleIterator;
import org.apache.hyracks.storage.am.buffertree.api.IPrimaryBufferBucket;
import org.apache.hyracks.storage.am.buffertree.api.IPrimaryBufferBucketFactory;
import org.apache.hyracks.storage.am.buffertree.api.ITupleIterator;
import org.apache.hyracks.storage.am.buffertree.api.frames.IPrimaryBufferFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.IPrimaryBufferFrameFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class PrimaryBufferBucket implements IPrimaryBufferBucket {

    protected final int numPages;
    protected final int pageSize;
    protected final IPrimaryBufferFrameFactory frameFactory;
    protected int memFileId;
    protected int memFileOffset;
    protected IBufferCache bufferCache;
    protected ICachedPage[] pages;
    protected IPrimaryBufferFrame[] frames;
    protected PriorityQueueIterator iterator;

    protected AtomicInteger currentPage;
    protected AtomicInteger serialId;
    protected AtomicBoolean isValid;
    protected AtomicBoolean isEmptying;
    protected ReentrantReadWriteLock readWriteLock;

    public PrimaryBufferBucket(int numPages, int pageSize, IPrimaryBufferFrameFactory frameFactory) {
        this.numPages = numPages;
        this.pageSize = pageSize;
        this.currentPage = new AtomicInteger(0);
        this.serialId = new AtomicInteger(0);
        this.isValid = new AtomicBoolean(false);
        this.isEmptying = new AtomicBoolean(false);
        this.frameFactory = frameFactory;
        this.iterator = null;
        this.frames = new IPrimaryBufferFrame[numPages];
        this.pages = new ICachedPage[numPages];
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    public void initialize(IBufferCache bufferCache, int memFileId, int memFileOffset) {
        this.bufferCache = bufferCache;
        this.memFileId = memFileId;
        this.memFileOffset = memFileOffset;
    }

    public void open() {
        acquireWriteLock();
        try {
            for(int i = 0; i < numPages; i++) {
                pages[i] = bufferCache.pinVirtual(BufferedFileHandle.getDiskPageId(memFileId, memFileOffset + i));
                frames[i] = frameFactory.createFrame();
                frames[i].setPage(pages[i]);
                frames[i].initBuffer(serialId.incrementAndGet());
            }
        } catch (HyracksDataException e) {
            e.printStackTrace();
        } finally {
            releaseWriteLock();
        }
    }

    public ITupleReference getMatchingKeyTuple(ITupleReference searchKey) throws HyracksDataException {
        acquireReadLock();
        ITupleReference temp = null;
        int endIndex;

        if(isEmptying.get() || isValid.get()) {
            endIndex = currentPage.get();
            for(int i = 0; i <= endIndex; i++) {
                temp = frames[i].getMatchingKeyTuple(searchKey);
                if(temp != null) {
                    releaseReadLock();
                    return  temp;
                }
            }
        }

        releaseReadLock();
        return null;
    }

    public FrameOpStatus insertIntoBuffer(ITupleReference tuple) throws HyracksDataException {
        acquireWriteLock();
        //acquire write lock
        int currentPageId = currentPage.get();
        FrameOpStatus opStatus = frames[currentPageId].insert(tuple);
        if(opStatus != FrameOpStatus.COMPLETED && currentPageId < numPages - 1) {
            //get the new index
            currentPageId = currentPage.incrementAndGet();
            //try inserting now
            opStatus = frames[currentPageId].insert(tuple);
            if(opStatus != FrameOpStatus.COMPLETED) {
                //new page must have space for atleast one tuple
                throw new AssertionError();
            }
        }
        //if failed and all pages are used, it returns failed status directly
        releaseWriteLock();
        return opStatus;
    }

    public FrameOpStatus insertIntoBuffer(ITupleIterator iterator) throws HyracksDataException {
        acquireWriteLock();
        //acquire write lock
        int currentPageId = currentPage.get();
        FrameOpStatus opStatus = frames[currentPageId].insert(iterator);

        //while loop because it is iterator and can have many tuples!
        while(opStatus != FrameOpStatus.COMPLETED && currentPageId < numPages - 1) {
            //get the new index
            currentPageId = currentPage.incrementAndGet();
            //try inserting now
            opStatus = frames[currentPageId].insert(iterator);
        }
        //if failed and all pages are used, it returns failed status directly
        releaseWriteLock();
        return opStatus;
    }

    public boolean isValid() {
        return isValid.get();
    }

    public boolean isEmptying() {
        return isEmptying.get();
    }

    public void setEmptyingStatus(boolean emptying) {
        acquireWriteLock();
        if(!isEmptying.compareAndSet(!emptying, emptying)) {
            throw new AssertionError();
        }
        releaseWriteLock();
    }

    private void setValidity(boolean valid) {
        acquireWriteLock();
        if(!isValid.compareAndSet(!valid, valid)) {
            throw new AssertionError();
        }
        releaseWriteLock();
    }

    public IOrderedTupleIterator getIterator() {
        if (!isValid.get()) throw new AssertionError();

        if(iterator == null) {
            iterator = new PriorityQueueIterator(numPages);
        } else {
            iterator.reset();
        }

        for(int i = 0; i <= currentPage.get(); i++) {
            iterator.pushIntoPriorityQueue(frames[i].getOrderedIterator());
        }
        return iterator;
    }

    public void addIterators(PriorityQueueIterator pqIterator) {
        for(int i = 0; i <= currentPage.get(); i++) {
            pqIterator.pushIntoPriorityQueue(frames[i].getOrderedIterator());
        }
    }

    public void markValid() {
        setValidity(true);
    }

    public void markInvalid() {
        setValidity(false);
    }

    public int getSize() {
        return pageSize * numPages;
    }

    public void reset() {
        acquireWriteLock();
        for(int i = 0; i < numPages; i++) {
            if(frames[i] != null) {
                frames[i].reset();
            }
        }
        releaseWriteLock();
    }

    public void acquireReadLock() {
        readWriteLock.readLock().lock();
    }

    public void acquireWriteLock() {
        readWriteLock.writeLock().lock();
    }

    public void releaseReadLock() {
        readWriteLock.readLock().unlock();
    }

    public void releaseWriteLock() {
        readWriteLock.writeLock().unlock();
    }

    public void close() {
        try {
            for(int i = 0; i < numPages; i++) {
                bufferCache.unpin(pages[i]);
            }
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }
    }

    public static class Factory implements IPrimaryBufferBucketFactory {

        protected final int numPages;
        protected final int pageSize;
        protected final IPrimaryBufferFrameFactory frameFactory;

        public Factory(int numPages, int pageSize, IPrimaryBufferFrameFactory frameFactory) {
            this.numPages = numPages;
            this.pageSize = pageSize;
            this.frameFactory = frameFactory;
        }

        public IPrimaryBufferBucket createBucket() {
            return new PrimaryBufferBucket(numPages, pageSize, frameFactory);
        }
    }
}
