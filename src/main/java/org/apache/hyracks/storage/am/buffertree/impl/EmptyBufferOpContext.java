package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.api.frames.*;
import org.apache.hyracks.storage.am.buffertree.frames.TempCachedPage;

import java.util.Stack;

public class EmptyBufferOpContext {

    public BufferTree tree;
    //used for the node iterators and one leaf frame
    public ITreeNodeFrame rootNodeFrame;
    public Stack<ITreeNodeWithBufferFrame> interiorNodeFrames;
    public ITreeLeafFrame leafFrame;

    //root buffers
    public IOrderedTupleIterator bufferIterator;
    public IOrderedTupleIterator childrenIterator;
    public IIndexTupleWithBloomFilter bfIndexTuple;
    public TempCachedPage tempCachedPage;
    public int writeLatchCount;
    public int readLatchCount;

    public EmptyBufferOpContext(BufferTree tree) throws HyracksDataException {
        this.tree = tree;
        this.tempCachedPage = new TempCachedPage(tree.bufferCache.getPageSize());
        writeLatchCount = 0;
        readLatchCount = 0;
        initFrames();
    }

    protected void initFrames() {
        int height = tree.metaFrame.getHeight();
        //insert node iterators, and leaf iterators
        rootNodeFrame = tree.rootNodeFrameFactory.createFrame();
        interiorNodeFrames = new Stack<ITreeNodeWithBufferFrame>();
        for(int i = height -2; i >= 1; i++) {
            interiorNodeFrames.push(tree.nodeFrameFactory.createFrame(i));
        }
        leafFrame = tree.leafFrameFactory.createFrame();
    }

    public void resetStackSize() {
        int height = tree.metaFrame.getHeight();
        //change the stack size
        if(height - 2 < interiorNodeFrames.size()) {
            //we are good
        } else {
            for(int i = height -2; i >= interiorNodeFrames.size(); i--) {
                interiorNodeFrames.push(tree.nodeFrameFactory.createFrame(i));
            }
        }
    }

    public void reset() {
        tempCachedPage.reset();
        writeLatchCount = 0;
        readLatchCount = 0;
    }
}
