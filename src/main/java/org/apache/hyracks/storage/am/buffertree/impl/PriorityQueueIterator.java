package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.IOrderedTupleIterator;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueComparator;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTuple;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTupleWriter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class PriorityQueueIterator implements IOrderedTupleIterator {

    ArrayList<IOrderedTupleIterator> iterators;
    PriorityQueue<IOrderedTupleIterator> queue;
    IReusableTupleReference endTuple;

    public PriorityQueueIterator(int numSize) {
        this.iterators = new ArrayList<IOrderedTupleIterator>();
        this.queue = new PriorityQueue<IOrderedTupleIterator>(numSize, OrderedIteratorComparator.INSTANCE);
        this.endTuple = KeyValueTupleWriter.INSTANCE.createNewTuple();
        this.endTuple.deepCopy(KeyValueTuple.HIGHEST_KEY_TUPLE);
    }

    public void seek(ITupleReference tuple) throws HyracksDataException {
        for (int i = 0; i < iterators.size(); i++) {
            IOrderedTupleIterator currentIterator = iterators.get(i);
            if (queue.contains(currentIterator)) {
                queue.remove(currentIterator);
                currentIterator.seek(tuple);
                queue.add(currentIterator);
            }
        }
    }

    public void pushIntoPriorityQueue(IOrderedTupleIterator newIterator) {
        if(!newIterator.done()) {
            iterators.add(newIterator);
            queue.add(newIterator);
        }
    }

    public void setEndLimit(ITupleReference limit) throws HyracksDataException {
        endTuple.reset(limit);
        for (int i = 0; i < iterators.size(); i++) {
            IOrderedTupleIterator currentIterator = iterators.get(i);
            if (queue.contains(currentIterator)) {
                queue.remove(currentIterator);
                currentIterator.setEndLimit(limit);
                queue.add(currentIterator);
            }
        }
    }

    public ITupleReference getCurrentLimit() throws HyracksDataException {
        return endTuple;
    }

    public boolean hasNext() throws HyracksDataException {
        if (queue.size() > 0) {
            return queue.peek().hasNext();
        } else {
            return false;
        }
    }

    public void next() throws HyracksDataException {
        IOrderedTupleIterator iterator = queue.poll();
        iterator.next();

        if (!iterator.done()) {
            queue.add(iterator);
        } else {
            iterators.remove(iterator);
        }
    }

    public ITupleReference peek() throws HyracksDataException {
        if (queue.size() > 0) {
            return queue.peek().peek();
        } else {
            return null;
        }
    }

    public int getPeekOffset() {
        throw new AssertionError();
    }

    public void unsafeUpdate(ITupleReference tuple, int off) {
        throw new AssertionError();
    }

    public int estimateSize() throws HyracksDataException {
        int size = 0;
        Iterator<IOrderedTupleIterator> pqIterator = queue.iterator();
        while (pqIterator.hasNext()) {
            size += pqIterator.next().estimateSize();
        }
        return size;
    }

    public void reset() {
        iterators.clear();
        queue.clear();
        endTuple.deepCopy(KeyValueTuple.HIGHEST_KEY_TUPLE);
    }

    public boolean done() {
        return queue.size() == 0;
    }

    public static class OrderedIteratorComparator implements Comparator<IOrderedTupleIterator> {

        public static OrderedIteratorComparator INSTANCE = new OrderedIteratorComparator();

        public int compare(IOrderedTupleIterator o1, IOrderedTupleIterator o2) {
            try {
                if(o1.hasNext() && o2.hasNext()) {
                    return KeyValueComparator.INSTANCE.compare(o1.peek(), o2.peek());
                } else if(o1.hasNext() && !o2.hasNext()) {
                    return -1;
                } else if(o2.hasNext() && !o1.hasNext()) {
                    return 1;
                } else {
                    return  0;
                }
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
            return 0;
        }

    }
}
