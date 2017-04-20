package org.apache.hyracks.storage.am.buffertree.impl;

import com.google.common.hash.BloomFilter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeLeafFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeNodeFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeNodeWithBufferFrame;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueIndexTupleWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTuple;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * Point search is available only for unique key trees.
 */
public class PointSearchCursor implements IIndexCursor {

    protected BufferTree tree;
    protected IReusableTupleReference matchingKeyValue;
    protected PointSearchPredicate pred;

    public PointSearchCursor(BufferTree tree) {
        this.tree = tree;
    }

    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws IndexException, HyracksDataException {
        //do not use initial state
        matchingKeyValue = null;
    }

    public boolean hasNext() throws HyracksDataException {
        return matchingKeyValue != null;
    }

    public void next() throws HyracksDataException {
        matchingKeyValue = null;
    }

    public void close() throws HyracksDataException {
        this.tree = null;
        this.pred = null;
        this.matchingKeyValue = null;
    }

    public void reset() throws HyracksDataException {
        this.pred = null;
        this.matchingKeyValue = null;
    }

    public ITupleReference getTuple() {
        return matchingKeyValue;
    }
}
