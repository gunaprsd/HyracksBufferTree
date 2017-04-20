package org.apache.hyracks.storage.am.buffertree.impl;


import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;

import java.nio.ByteBuffer;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.ICachedPageFrame;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTuple;

public class SlottedTupleManager implements ISlottedTupleManager{

    public static int GREATEST_TUPLE_INDICATOR = -2;
    public static int FIND_INDEX_ERROR = -3;

    //frame variables
    protected final ICachedPageFrame frame;
    protected final ITupleWriter writer;
    protected final ITupleComparator comparator;

    //added here to reuse the same frame for different levels
    protected int startOffset;
    protected int endOffset;
    //state variables
    protected IOrderedTupleIterator iterator = null;
    protected IBulkTupleInserter bulkInserter = null;
    protected IReusableTupleReference tempTuple = null;


    public static final int startOfFreeSpaceOff = 0;
    public static final int endOfFreeSpaceOff = startOfFreeSpaceOff + 4;
    public static final int managerHeaderSize = endOfFreeSpaceOff + 4;

    /**
     * Constructor for a slotted tuple manager.
     * @param frame underlying frame for the manager
     * @param startOffset start offset of the space that the manager has to manage
     * @param endOffset endOff offset of the space that the manager has to manage
     * @param writer writer used to write a tuple
     * @param comparator comparator used for comparing tuples
     */
    public SlottedTupleManager(ICachedPageFrame frame, int startOffset, int endOffset, ITupleWriter writer, ITupleComparator comparator) {
        this.frame = frame;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.writer = writer;
        this.comparator = comparator;
        this.tempTuple = writer.createNewTuple();
    }

    /**
     * Function to initialize a new buffer
     */
    public void initParams() {
        setStartOfFreeSpace(startOffset + managerHeaderSize);
        setEndOfFreeSpace(endOffset);
    }

    /**
     * Accessor function for start offset
     * @return
     */
    public int getStartOffset() {
        return startOffset;
    }

    public int getHeaderSize() {
        return managerHeaderSize;
    }

    /**
     * Accessor function for endOff offset
     * @return
     */
    public int getEndOffset() {
        return endOffset;
    }

    /**
     * Accessor function for start offset of the slots
     * @return
     */
    public int getStartSlotOffset() {
        return startOffset + managerHeaderSize;
    }

    /**
     * Returns the writer
     * @return
     */
    public ITupleWriter getWriter() {
        return writer;
    }

    /**
     * Returns the comparator
     * @return
     */
    public ITupleComparator getComparator() {
        return comparator;
    }

    public int getStartOfFreeSpace() {
        return frame.getBuffer().getInt(startOffset + startOfFreeSpaceOff);
    }

    public void setStartOfFreeSpace(int startOfFreeSpace) {
        frame.getBuffer().putInt(startOffset + startOfFreeSpaceOff, startOfFreeSpace);
    }

    public int getEndOfFreeSpace() {
        return frame.getBuffer().getInt(startOffset + endOfFreeSpaceOff);
    }

    public void setEndOfFreeSpace(int endOfFreeSpace) {
        frame.getBuffer().putInt(startOffset + endOfFreeSpaceOff, endOfFreeSpace);
    }


    public ByteBuffer getBuffer() {
        return frame.getBuffer();
    }

    /**
     * Computes the tuple count
     * @return
     */
    public int getTupleCount() {
        int s = getStartOfFreeSpace();
        int t = getStartSlotOffset();
        int u = getSlotSize();
        return (getStartOfFreeSpace()- getStartSlotOffset())/getSlotSize();
    }

    /**
     * Computes the free space in the frame
     * @return
     */
    public int getFreeSpace() {
        int e = getEndOfFreeSpace();
        int s = getStartOfFreeSpace();
        return getEndOfFreeSpace() - getStartOfFreeSpace();
    }

    /**
     * Resets the parameters of the parameters - parameters resemble empty frame
     */
    public void resetParams() {
        initParams();
        int e = getEndOfFreeSpace();
        int s = getStartOfFreeSpace();
    }

    /**
     * Computes the bytes required to add the tuple in the frame: includes slot size and the tuple size
     * @param tuple
     * @return
     */
    public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
        return writer.bytesRequired(tuple) + getSlotSize();
    }

    /**
     * Returns the offset of the tuple in the frame based on the index. Index starts at zero.
     * @param index
     * @return
     */
    public int getTupleOffsetByIndex(int index) {return getTupleOffset(getSlotOff(index));}

    /**
     * Returns the offset of the tuple in the frame based on the slot offset.
     * @param slotOffset
     * @return
     */
    public int getTupleOffset(int slotOffset) {
        return frame.getBuffer().getInt(slotOffset);
    }

    /**
     * Returns the slot size
     * @return
     */
    public int getSlotSize() {
        return 4;
    }

    /**
     * Returns the slot offset of the tuple based on the index
     * @param index
     * @return
     */
    public int getSlotOff(int index) {return getStartSlotOffset() + (getSlotSize() * index);}

    /**
     * Returns the tuple at the given offset
     * @param off
     * @return
     */
    public ITupleReference getTupleByOffset(int off) {
        tempTuple.reset(frame.getBuffer(), off);
        ITupleReference tuple = writer.createNewTuple(tempTuple);
        return tuple;
    }

    /**
     * Return the tuple using the index
     * @param index
     * @return
     */
    public ITupleReference getTupleByIndex(int index) {
        if (index < 0) throw new AssertionError();
        return getTupleByOffset(getTupleOffsetByIndex(index));
    }

    /**
     * Finds the index of the tuple based on searchTuple and the mode. In all the modes, it returns
     * "one" of the tuple satisfying the mode criterion.
     * @param searchTuple
     * @param mode
     * @return
     * @throws HyracksDataException
     */
    public int findTupleIndex(ITupleReference searchTuple, FindTupleMode mode) throws HyracksDataException {
        int tupleCount = getTupleCount();
        if(tupleCount <= 0) {
            if(mode == FindTupleMode.EXACT) {
                return FIND_INDEX_ERROR;
            } else {
                return GREATEST_TUPLE_INDICATOR;
            }
        }

        int begin, mid, end;
        end = tupleCount -1;

        int tupleOffset = getTupleOffsetByIndex(end);
        tempTuple.reset(frame.getBuffer(), getTupleOffsetByIndex(end));
        int cmp = comparator.compare(searchTuple, tempTuple);
        if(cmp > 0) {
            //special optimization for search tuple greater than all
            begin = tupleCount;
        } else {
            begin = 0;
        }

        //binary search based on search tuple
        while(begin <= end) {
            mid = (begin + end)/ 2;
            tempTuple.reset(frame.getBuffer(), getTupleOffsetByIndex(mid));

            cmp = comparator.compare(searchTuple, tempTuple);
            if(cmp > 0) {
                begin = mid + 1;
            } else if(cmp < 0) {
                end = mid - 1;
            } else {
                if(mode == FindTupleMode.EXACT || mode == FindTupleMode.LESS_THAN_EQUALS) {
                    return mid;
                } else if(mode == FindTupleMode.GREATER_THAN) {
                    begin = mid + 1;
                }
            }
        }

        if(mode == FindTupleMode.EXACT) {
            return FIND_INDEX_ERROR;
        } else if(mode == FindTupleMode.GREATER_THAN) {
            if(begin > tupleCount - 1) {
                return GREATEST_TUPLE_INDICATOR;
            } else {
                return begin;
            }
        } else if(mode == FindTupleMode.LESS_THAN_EQUALS) {
            if (end < 0) {
                return FIND_INDEX_ERROR;
            } else {
                return end;
            }
        } else {
            return FIND_INDEX_ERROR;
        }
    }

    /**
     * Insert a new tuple into the frame
     * @param tuple
     * @return
     * @throws HyracksDataException
     */
    public FrameOpStatus insertTuple(ITupleReference tuple) throws HyracksDataException {
        int freeSpace = getFreeSpace();
        if(getBytesRequiredToWriteTuple(tuple) <= getFreeSpace()) {
            int index = findTupleIndex(tuple, FindTupleMode.GREATER_THAN);
            int tupleLength = writer.bytesRequired(tuple);
            int endOfFreeSpace = getEndOfFreeSpace();
            endOfFreeSpace -= tupleLength;
            //insert the slot
            insertSlot(index, endOfFreeSpace);
            //write the tuple
            writer.writeTuple(tuple, frame.getBuffer(), endOfFreeSpace);
            setEndOfFreeSpace(endOfFreeSpace);
            return FrameOpStatus.COMPLETED;
        } else {
            return FrameOpStatus.FAIL;
        }
    }

    /**
     * Returns the matching tuple based on the search key
     * @param searchTuple
     * @return
     * @throws HyracksDataException
     */
    public ITupleReference getMatchingKeyTuple(ITupleReference searchTuple) throws HyracksDataException {
        int index = findTupleIndex(searchTuple, FindTupleMode.EXACT);
        if(index != FIND_INDEX_ERROR) {
            return getTupleByIndex(index);
        } else {
            return null;
        }
    }

    /**
     * Inserts the slot at the slotIndex
     * @param slotIndex
     * @param value
     */
    public void insertSlot(int slotIndex, int value) {
        if (slotIndex == SlottedTupleManager.FIND_INDEX_ERROR) throw new AssertionError();

        int startOfFreeSpace = getStartOfFreeSpace();
        if(slotIndex == SlottedTupleManager.GREATEST_TUPLE_INDICATOR) {
            frame.getBuffer().putInt(getStartOfFreeSpace(), value);
        } else {
            //shift other slots
            int slotOffset = getSlotOff(slotIndex);
            System.arraycopy(frame.getBuffer().array(), slotOffset, frame.getBuffer().array(), slotOffset + getSlotSize(), startOfFreeSpace  - slotOffset);
            frame.getBuffer().putInt(slotOffset, value);
        }
        setStartOfFreeSpace(startOfFreeSpace + getSlotSize());
    }

    public void resetState() {
        if(iterator != null) {
            iterator.reset();
        }
        if(bulkInserter != null) {
            bulkInserter.reset();
        }
        if(tempTuple != null) {
            tempTuple.reset();
        }
    }

    public void resetSizeOffsets(int startOffset, int endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public ITupleIterator getIterator() {
        if(iterator == null) {
            iterator = new OrderedTupleIterator(this);
        } else {
            iterator.reset();
        }
        return iterator;
    }

    public IOrderedTupleIterator getOrderedIterator() {
        return (IOrderedTupleIterator)getIterator();
    }

    /**
     * Must be used only for empty iterators
     * @return
     */
    public IBulkTupleInserter getBulkInserter() {
        if(bulkInserter == null) {
            bulkInserter = new BulkInserter(this);
        } else {
            bulkInserter.reset();
        }
        return bulkInserter;
    }

    /**
     * Bulk inserter mainly used in leaf/node splitting. Inserting tuples must be in the
     * sorted order. Verified.
     */
    public static class BulkInserter implements IBulkTupleInserter {

        SlottedTupleManager manager;
        int startOfFreeSpace;
        int endOfFreeSpace;

        public BulkInserter(SlottedTupleManager manager) {
            this.manager = manager;
        }

        public void init() {
            startOfFreeSpace = manager.getStartOfFreeSpace();
            endOfFreeSpace = manager.getEndOfFreeSpace();
            if (startOfFreeSpace != manager.getStartSlotOffset()) throw new AssertionError();
        }

        public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
            return manager.getBytesRequiredToWriteTuple(tuple);
        }

        public FrameOpStatus insert(ITupleReference tuple) {
            if(manager.getBytesRequiredToWriteTuple(tuple) > (endOfFreeSpace - startOfFreeSpace)) {
                return FrameOpStatus.FAIL;
            } else {
                endOfFreeSpace -= manager.writer.bytesRequired(tuple);
                manager.writer.writeTuple(tuple, manager.getBuffer(), endOfFreeSpace);
                manager.getBuffer().putInt(startOfFreeSpace, endOfFreeSpace);
                startOfFreeSpace += manager.getSlotSize();
                return FrameOpStatus.COMPLETED;
            }
        }

        public void end() {
            manager.setStartOfFreeSpace(startOfFreeSpace);
            manager.setEndOfFreeSpace(endOfFreeSpace);
        }

        public void reset() {
            init();
        }
    }

    /**
     * An ordered tuple bufferIterator on the tuples
     */
    public static class OrderedTupleIterator implements  IOrderedTupleIterator {

        /**
         * Iterator invariants:
         * 1. Iterator position is described by the current tuple : can be invoked by calling peek()
         * 2. nextOff is the offset of the next tuple in the bufferIterator
         * 3. hasNext denotes if there is a current valid tuple and if it obeys the endTuple constraint
         * 4. done() is satisfied only when all the tuples in the bufferIterator irrespective of the tuple
         *    constraints are read and next is invoked.
         */
        SlottedTupleManager manager;
        IReusableTupleReference current;
        IReusableTupleReference endTuple;
        int endOff;
        int nextOff;
        boolean hasNext;

        public OrderedTupleIterator(SlottedTupleManager manager, int start, int end) {
            this.manager = manager;
            this.nextOff = start;
            this.endOff = end;
            this.hasNext = true;
            this.current = manager.writer.createNewTuple();
            this.endTuple = KeyValueTuple.HIGHEST_KEY_TUPLE;
            next();
        }

        public OrderedTupleIterator(SlottedTupleManager manager) {
            this(manager, manager.getStartSlotOffset(), manager.getStartOfFreeSpace());
        }

        public boolean hasNext() throws HyracksDataException {
            return hasNext;
        }

        public void next() {
            if(nextOff < endOff) {
                current.reset(manager.getBuffer(), manager.getTupleOffset(nextOff));
                nextOff += manager.getSlotSize();
                hasNext = manager.comparator.compare(current, endTuple) < 0;
            } else if(nextOff == endOff) {
                nextOff += manager.getSlotSize();
                hasNext = false;
            }
        }

        public ITupleReference peek() throws HyracksDataException {
            if(hasNext) {
                return current;
            } else {
                return null;
            }
        }

        public int getPeekOffset() {
            return manager.getTupleOffset(nextOff - manager.getSlotSize());
        }

        //TODO not very safe!
        public void unsafeUpdate(ITupleReference tuple, int off) {
            manager.writer.writeTuple(tuple, manager.getBuffer(), off);
        }

        public int estimateSize() throws HyracksDataException {
            int tempOff = nextOff;
            boolean tempHasNext = hasNext;
            ITupleReference tempCurrent = manager.writer.createNewTuple(current);

            int size = 0;
            while(hasNext()) {
                size += manager.getBytesRequiredToWriteTuple(peek());
                next();
            }

            nextOff = tempOff;
            hasNext = tempHasNext;
            current.reset(tempCurrent);
            return size;
        }

        public void reset() {
            this.nextOff = manager.getStartSlotOffset();
            this.endOff = manager.getStartOfFreeSpace();
            this.hasNext = true;
            this.endTuple = KeyValueTuple.HIGHEST_KEY_TUPLE;
            next();
        }

        public boolean done() {
            return nextOff > endOff;
        }

        public void seek(ITupleReference tuple) throws HyracksDataException {
            int index = manager.findTupleIndex(tuple, FindTupleMode.LESS_THAN_EQUALS);
            if(index == GREATEST_TUPLE_INDICATOR) {
                nextOff = endOff + 1;
                hasNext = false;
            } else {
                nextOff = index == FIND_INDEX_ERROR ? manager.getStartSlotOffset() : manager.getSlotOff(index);
                current.reset(manager.getBuffer(), nextOff);
                nextOff += manager.getSlotSize();
                hasNext = true;
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

