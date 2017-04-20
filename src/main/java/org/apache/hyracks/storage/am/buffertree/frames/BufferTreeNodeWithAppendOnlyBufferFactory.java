package org.apache.hyracks.storage.am.buffertree.frames;

import org.apache.hyracks.storage.am.buffertree.api.ITupleComparator;
import org.apache.hyracks.storage.am.buffertree.api.ITupleWriter;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeNodeWithBufferFrame;
import org.apache.hyracks.storage.am.buffertree.api.frames.ITreeNodeWithBufferFrameFactory;

public class BufferTreeNodeWithAppendOnlyBufferFactory implements ITreeNodeWithBufferFrameFactory {

    private int nodeSize;
    private int bufferSize;
    private ITupleWriter nodeWriter;
    private ITupleWriter bufferWriter;
    private ITupleComparator comparator;

    public BufferTreeNodeWithAppendOnlyBufferFactory(int nodeSize, int bufferSize, ITupleWriter nodeWriter, ITupleWriter bufferWriter, ITupleComparator comparator) {
        this.nodeSize = nodeSize;
        this.bufferSize = bufferSize;
        this.nodeWriter = nodeWriter;
        this.bufferWriter = bufferWriter;
        this.comparator = comparator;
    }

    public ITreeNodeWithBufferFrame createFrame(int level) {
        return new BufferTreeNodeWithBufferFrame(nodeSize, bufferSize, nodeWriter, bufferWriter, comparator, false);
    }

    public int getNodeCapacity(int level) {
        return nodeSize;
    }

    public int getBufferCapacity(int level) {
        return bufferSize;
    }
}
