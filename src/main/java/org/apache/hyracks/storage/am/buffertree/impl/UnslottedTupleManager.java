package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.ICachedPageFrame;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTuple;

import java.nio.ByteBuffer;

//TODO
public class UnslottedTupleManager implements ITupleManager {

    protected final ITupleWriter writer;
    protected final ICachedPageFrame frame;
    protected final ITupleComparator comparator;

    protected int startOffset;
    protected int endOffset;
    protected ITupleIterator iterator = null;
    protected IOrderedTupleIterator orderedIterator = null;
    protected IBulkTupleInserter bulkInserter = null;
    protected IReusableTupleReference managerTuple = null;

    public static final int startOfFreeSpaceOff = 0;
    public static final int managerHeaderSize = startOfFreeSpaceOff + 4;

    public UnslottedTupleManager(ICachedPageFrame frame, int startOffset, int endOffset, ITupleWriter writer, ITupleComparator comparator) {
        this.frame = frame;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.writer = writer;
        this.managerTuple = writer.createNewTuple();
        this.comparator = comparator;
        this.managerTuple = writer.createNewTuple();
    }

    public void initParams() {
       setStartOfFreeSpace(startOffset + managerHeaderSize);
    }

    protected int getStartOfFreeSpace() {
        return frame.getBuffer().getInt(startOffset + startOfFreeSpaceOff);
    }

    protected void setStartOfFreeSpace(int startOfFreeSpace) {
        frame.getBuffer().putInt(startOffset + startOfFreeSpaceOff,  startOfFreeSpace);
    }

    public ByteBuffer getBuffer() {
        return frame.getBuffer();
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getHeaderSize() {
        return managerHeaderSize;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public int getTupleCount() {
        int count = 0;
        int offset = getStartOffset() + getHeaderSize();
        int maxOffset = getStartOfFreeSpace();

        while(offset < maxOffset) {
            managerTuple.reset(frame.getBuffer(), offset);
            offset += writer.bytesRequired(managerTuple);
            count++;
        }

        if (offset != maxOffset) throw new AssertionError();

        return count;
    }

    public int getFreeSpace() {
        return getEndOffset() - getStartOfFreeSpace();
    }

    public void resetParams() {
        initParams();
    }

    public void resetState() {
        if(iterator != null) iterator.reset();
        if(orderedIterator != null) orderedIterator.reset();
        if(bulkInserter != null) bulkInserter.reset();
        if(managerTuple != null) managerTuple.reset();
    }

    public void resetSizeOffsets(int startOffset, int endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
        return writer.bytesRequired(tuple);
    }

    public FrameOpStatus insertTuple(ITupleReference tuple) {
        int len = getBytesRequiredToWriteTuple(tuple);
        if(getFreeSpace() < len) {
            return FrameOpStatus.FAIL;
        } else {
            int startOfFreeSpace = getStartOfFreeSpace();
            frame.getBuffer().position(startOfFreeSpace);
            writer.writeTuple(tuple, frame.getBuffer(), startOfFreeSpace);
            setStartOfFreeSpace(startOfFreeSpace + len);
            return FrameOpStatus.COMPLETED;
        }
    }

    public ITupleReference getMatchingKeyTuple(ITupleReference searchTuple) throws HyracksDataException {
        int offset = startOffset, maxOffset = getStartOfFreeSpace();

        while(offset < maxOffset) {
            managerTuple.reset(frame.getBuffer(), offset);
            if(comparator.compare(managerTuple, searchTuple) == 0) {
                return managerTuple;
            }
            offset += writer.bytesRequired(managerTuple);
        }

        return null;
    }

    public ITupleIterator getIterator() {
        if(iterator == null) {
            iterator = getExclusiveIterator();
        } else {
            //ensure that the contents have not changed since it was created.
            iterator.reset();
        }
        return iterator;
    }

    public ITupleIterator getExclusiveIterator() {
        return new TupleIterator(this);
    }

    public IOrderedTupleIterator getOrderedIterator() throws HyracksDataException {
        if(orderedIterator == null) {
            orderedIterator = new OrderedIterator(this);
        } else {
            orderedIterator.reset();
        }
        return orderedIterator;
    }

    public IBulkTupleInserter getBulkInserter() {
        if(bulkInserter == null) {
            bulkInserter = new BulkInserter(this);
        } else {
            bulkInserter.reset();
        }
        return bulkInserter;
    }

    public ITupleWriter getWriter() {
        return writer;
    }

    public ITupleComparator getComparator() {
        return comparator;
    }

    public static class BulkInserter implements IBulkTupleInserter {

        protected UnslottedTupleManager manager;
        protected int startOfFreeSpace;

        public BulkInserter(UnslottedTupleManager manager) {
            this.manager = manager;
        }

        public void init() {
            startOfFreeSpace = manager.getStartOfFreeSpace();
        }

        public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
            return manager.getBytesRequiredToWriteTuple(tuple);
        }

        public FrameOpStatus insert(ITupleReference tuple)  {
            if(manager.getBytesRequiredToWriteTuple(tuple) > (manager.getEndOffset() - startOfFreeSpace)) {
                return FrameOpStatus.FAIL;
            } else {
                int len = manager.getBytesRequiredToWriteTuple(tuple);
                manager.writer.writeTuple(tuple, manager.getBuffer(), startOfFreeSpace);
                startOfFreeSpace += len;
                return FrameOpStatus.COMPLETED;
            }
        }

        public void end() {
            manager.setStartOfFreeSpace(startOfFreeSpace);
        }

        public void reset() {
            startOfFreeSpace = manager.getStartOfFreeSpace();
        }
    }

    public static class TupleIterator implements ITupleIterator {

        protected UnslottedTupleManager manager;
        protected IReusableTupleReference current;
        protected int off;
        protected int end;
        protected boolean hasNext;

        public TupleIterator(UnslottedTupleManager manager) {
            this.manager = manager;
            this.off = manager.getStartOffset() + manager.getHeaderSize();
            this.end = manager.getStartOfFreeSpace();
            this.current = manager.writer.createNewTuple();
            this.hasNext = true;
            next();
        }

        public boolean hasNext() {
            return hasNext;
        }

        public void next() {
            if(off < end) {
                current.reset(manager.getBuffer(), off);
                off += manager.writer.bytesRequired(current);
                hasNext = true;
            } else {
                hasNext = false;
            }
        }

        public ITupleReference peek() {
            if(hasNext) {
                return current;
            } else {
                return null;
            }
        }

        public int getPeekOffset() {
            return off - manager.writer.bytesRequired(current);
        }

        //TODO not very safe!
        public void unsafeUpdate(ITupleReference tuple, int off) {
            manager.writer.writeTuple(tuple, manager.getBuffer(), off);
        }

        public int estimateSize() {
            int tempOff = off;
            boolean tempHasNext = hasNext;
            ITupleReference tempCurrent = manager.writer.createNewTuple(current);

            int size = 0;
            while(hasNext()) {
                size += manager.getBytesRequiredToWriteTuple(peek());
                next();
            }

            off = tempOff;
            hasNext = tempHasNext;
            current.reset(tempCurrent);
            return size;
        }

        public void reset() {
            this.off = manager.getStartOffset() + manager.getHeaderSize();
            this.end = manager.getStartOfFreeSpace();
            this.current.reset();
            this.hasNext = true;
            next();
        }

        public boolean done() {
            return off >= end && !hasNext;
        }
    }

    public static class OrderedIterator implements IOrderedTupleIterator {
        protected UnslottedTupleManager manager;
        protected int[] offsets;
        protected int endIndex;
        protected int nextIndex;
        protected IReusableTupleReference current;
        protected IReusableTupleReference endTuple;
        protected boolean hasNext;

        public OrderedIterator(UnslottedTupleManager manager) throws HyracksDataException {
            this.manager = manager;
            this.current = this.manager.writer.createNewTuple();
            this.endTuple = KeyValueTuple.HIGHEST_KEY_TUPLE;
            computeIndexArray();
            sortIndex();
            this.endIndex = offsets.length-1;
            this.nextIndex = 0;
            this.hasNext = true;
            next();
        }

        public boolean hasNext() throws HyracksDataException {
            return hasNext;
        }

        public void next() {
            if(nextIndex < endIndex) {
                current.reset(manager.getBuffer(), offsets[nextIndex]);
                nextIndex++;
                hasNext = manager.comparator.compare(current, endTuple) < 0;
            } else if(nextIndex == endIndex){
                nextIndex++;
                hasNext = false;
            }
        }

        public ITupleReference peek() {
            if(hasNext) {
                return current;
            } else {
                return null;
            }
        }

        public int getPeekOffset() {
            if (nextIndex < 0) throw new AssertionError();
            return offsets[nextIndex -1];
        }

        //TODO not very safe!
        public void unsafeUpdate(ITupleReference tuple, int off) {
            manager.writer.writeTuple(tuple, manager.getBuffer(), off);
        }


        public int estimateSize() throws HyracksDataException {
            int tempCur = nextIndex;
            boolean tempHasNext = hasNext;
            ITupleReference tempCurrent = manager.writer.createNewTuple(current);

            int size = 0;
            while(hasNext()) {
                size += manager.getBytesRequiredToWriteTuple(peek());
                next();
            }

            nextIndex = tempCur;
            hasNext = tempHasNext;
            current.reset(tempCurrent);
            return size;
        }

        public void reset() {
            computeIndexArray();
            try {
                sortIndex();
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
            this.nextIndex = 0;
            this.endIndex = offsets.length-1;
            this.hasNext = true;
            this.endTuple = KeyValueTuple.HIGHEST_KEY_TUPLE;
            next();
        }

        public boolean done() {
            return nextIndex > endIndex;
        }

        private void computeIndexArray() {
            int count = manager.getTupleCount();
            int offset = manager.getStartOffset() + manager.getHeaderSize();

            offsets = new int[count];
            for (int i = 0; i < count; i++) {
                offsets[i] = offset;
                current.reset(manager.getBuffer(), offset);
                offset += manager.writer.bytesRequired(current);
            }
        }

        private void sortIndex() throws HyracksDataException {
            IReusableTupleReference kv1 = manager.writer.createNewTuple();
            IReusableTupleReference kv2 = manager.writer.createNewTuple();
            for(int i = 0; i < offsets.length; i++) {
                for(int j = 1; j < offsets.length - i; j++) {
                    kv1.reset(manager.getBuffer(), offsets[j - 1]);
                    kv2.reset(manager.getBuffer(), offsets[j]);
                    int cmp = manager.comparator.compare(kv1, kv2);
                    if(cmp > 0) {
                        int temp = offsets[j-1];
                        offsets[j-1] = offsets[j];
                        offsets[j] = temp;
                    }
                }
            }
        }

        public void seek(ITupleReference tuple) throws HyracksDataException {
            for(int i = 0; i < offsets.length; i++) {
                current.reset(manager.getBuffer(), offsets[nextIndex]);
                if (manager.comparator.compare(tuple, current) <= 0) {
                    nextIndex = i + 1;
                }
            }
        }

        public void setEndLimit(ITupleReference tuple) {
            if(endTuple == KeyValueTuple.HIGHEST_KEY_TUPLE) {
                endTuple = manager.writer.createNewTuple(tuple);
            } else {
                endTuple.reset(tuple);
            }

            if(!done()) {
                hasNext = manager.comparator.compare(current, endTuple) < 0;
            }
        }

        public ITupleReference getCurrentLimit() throws HyracksDataException {
            return endTuple;
        }
    }

}
