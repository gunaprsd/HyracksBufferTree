package org.apache.hyracks.storage.am.buffertree.api.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;

import java.util.ArrayList;

public interface ITreeNodeWithBufferFrame extends ITreeBufferFrame, ITreeNodeFrame {

    ITupleReference getMatchingKeyInBuffer(ITupleReference searchKey) throws HyracksDataException;

    void resetSize(int nodeSize, int bufferSize);

    void distributeNodeAndBuffer(int splitSize, ArrayList<? extends ITreeNodeWithBufferFrame> rightFrame, ArrayList<? extends IIndexTupleReference> splitKey) throws HyracksDataException;

    void distributeNodeAndBuffer(int splitSize, ITreeNodeWithBufferFrame leftFrame, ITreeNodeWithBufferFrame rightFrame, IIndexTupleReference splitKey) throws HyracksDataException;
}
