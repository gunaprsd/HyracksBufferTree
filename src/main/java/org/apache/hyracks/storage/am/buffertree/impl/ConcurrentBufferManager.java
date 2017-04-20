package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentBufferManager implements IPrimaryBufferManager {

    protected final int numBuckets;
    protected final int numPagesPerBucket;
    protected final int pageSize;
    protected final IPrimaryBufferBucketFactory bucketFactory;
    protected BufferTree tree;
    protected int memFileId;
    protected AtomicInteger currentBucket;
    protected IPrimaryBufferBucket[] bufferBuckets;
    protected BufferEmptyThread emptyingThread;
    protected Executor executor;

    public ConcurrentBufferManager(int numBuckets, int numPagesPerBucket, int pageSize, IPrimaryBufferBucketFactory bucketFactory) {
        this.numBuckets = numBuckets;
        this.numPagesPerBucket = numPagesPerBucket;
        this.pageSize = pageSize;
        this.bucketFactory = bucketFactory;
        this.currentBucket = new AtomicInteger(-1);
    }

    public void initialize(BufferTree tree) {
        this.tree = tree;
        this.executor = Executors.newSingleThreadExecutor();
        this.emptyingThread = new BufferEmptyThread(this);
        this.bufferBuckets = new IPrimaryBufferBucket[numBuckets];
        try {
            this.memFileId = tree.bufferCache.createMemFile();
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }
    }

    protected int getFreeBucket() {
        int freeBucketId;
        if(currentBucket.compareAndSet(numBuckets - 1, 0)) {
            freeBucketId = 0;
        } else {
            freeBucketId = currentBucket.incrementAndGet();
        }

        //create a memfile
        if(bufferBuckets[freeBucketId] == null) {
            bufferBuckets[freeBucketId] = bucketFactory.createBucket();
            bufferBuckets[freeBucketId].initialize(tree.bufferCache, memFileId, freeBucketId * numPagesPerBucket);
            bufferBuckets[freeBucketId].open();
        } else if(bufferBuckets[freeBucketId].isEmptying()) {
            //no more buckets free. wait for some time!
            while (bufferBuckets[freeBucketId].isEmptying());
        }
        bufferBuckets[freeBucketId].markValid();
        return freeBucketId;
    }

    public void open() {
        getFreeBucket();
    }

    public IOrderedTupleIterator getIterator() {
        PriorityQueueIterator iterator = new PriorityQueueIterator(numBuckets);
        for(int i = 0; i < numBuckets; i++) {
            if(bufferBuckets[i] != null) {
                if(bufferBuckets[i].isValid()) {
                    bufferBuckets[i].addIterators(iterator);
                }
            }
        }
        return iterator;
    }

    public ITupleReference getMatchingKeyTuple(ITupleReference searchKey) throws HyracksDataException {
        ITupleReference temp;
        for(int i = 0; i < numBuckets; i++) {
            if(bufferBuckets[i] != null) {
                if(bufferBuckets[i].isValid()) {
                    temp = bufferBuckets[i].getMatchingKeyTuple(searchKey);
                    if(temp != null) {
                        return temp;
                    }
                }
            }
        }
        return null;
    }

    public void insert(ITupleIterator iterator) throws HyracksDataException {
        int currentBucketId = currentBucket.get();
        FrameOpStatus opStatus = bufferBuckets[currentBucketId].insertIntoBuffer(iterator);
        while(opStatus != FrameOpStatus.COMPLETED) {
            emptyingThread.submitJob(bufferBuckets[currentBucketId]);
            executor.execute(emptyingThread);
            currentBucketId = getFreeBucket();
            opStatus = bufferBuckets[currentBucketId].insertIntoBuffer(iterator);
        }
    }

    public void insert(ITupleReference tuple) throws HyracksDataException {
        int currentBucketId = currentBucket.get();
        FrameOpStatus opStatus = bufferBuckets[currentBucketId].insertIntoBuffer(tuple);
        if(opStatus != FrameOpStatus.COMPLETED) {
            emptyingThread.submitJob(bufferBuckets[currentBucketId]);
            //executor.execute(emptyingThread);
            emptyingThread.run();
            currentBucketId = getFreeBucket();
            opStatus = bufferBuckets[currentBucketId].insertIntoBuffer(tuple);
            if (opStatus != FrameOpStatus.COMPLETED) throw new AssertionError();
        }
    }

    public void close() {
        int currentBucketId = currentBucket.get();
        if(!bufferBuckets[currentBucketId].isEmptying()) {
            emptyingThread.submitJob(bufferBuckets[currentBucketId]);
            executor.execute(emptyingThread);
        }

        while(!emptyingThread.done());

        for(int i = 0; i < numBuckets; i++) {
            if(bufferBuckets[i] != null) {
                bufferBuckets[i].close();
            }
        }
    }

    public static class BufferEmptyThread extends Thread {

        protected final ConcurrentLinkedQueue<IPrimaryBufferBucket> jobQueue;
        protected final ConcurrentBufferManager bufferManager;
        protected PriorityQueueIterator iterator;
        protected EmptyBufferOpContext ctx;

        public BufferEmptyThread(ConcurrentBufferManager bufferManager) {
            this.jobQueue = new ConcurrentLinkedQueue<IPrimaryBufferBucket>();
            this.bufferManager = bufferManager;
            setPriority(MAX_PRIORITY);
            setDaemon(true);
        }

        public synchronized void submitJob(IPrimaryBufferBucket bucket) {
            bucket.setEmptyingStatus(true);
            jobQueue.add(bucket);
        }

        void emptyBufferBucket(IPrimaryBufferBucket bucket) throws HyracksDataException {
            if(iterator == null) {
                iterator = new PriorityQueueIterator(bufferManager.numPagesPerBucket);
            } else {
                iterator.reset();
            }
            bucket.addIterators(iterator);

            if(ctx == null) {
                ctx = new EmptyBufferOpContext(bufferManager.tree);
            } else {
                ctx.reset();
                ctx.resetStackSize();
            }

            bufferManager.tree.emptyPrimaryBuffer(ctx, iterator);
        }

        public synchronized boolean done() {
            return jobQueue.isEmpty();
        }

        @Override
        public synchronized void run() {
            while(!jobQueue.isEmpty()) {
                IPrimaryBufferBucket bucket = jobQueue.poll();
                if(bucket != null) {
                    try {
                        bucket.acquireReadLock();
                        emptyBufferBucket(bucket);
                        bucket.releaseReadLock();
                        bucket.reset();
                        bucket.markInvalid();
                        bucket.setEmptyingStatus(false);
                    } catch (HyracksDataException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
