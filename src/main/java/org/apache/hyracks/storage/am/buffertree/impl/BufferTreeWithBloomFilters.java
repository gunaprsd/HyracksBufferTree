package org.apache.hyracks.storage.am.buffertree.impl;

import com.google.common.hash.BloomFilter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.*;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueIndexTupleWithBloomFilter;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTuple;
import org.apache.hyracks.storage.am.common.api.IFreePageManager;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

import java.util.ArrayList;

public class BufferTreeWithBloomFilters extends BufferTree {

    protected final IBloomFilterFactory bloomFilterFactory;

    public BufferTreeWithBloomFilters(IBufferCache bufferCache, IFileMapProvider fileMapProvider, IFreePageManager freePageManager, FileReference file, ITreeNodeFrameFactory rootNodeFrameFactory, ITreeNodeWithBufferFrameFactory nodeFrameFactory, ITreeLeafFrameFactory leafFrameFactory, IPrimaryBufferFrameFactory bufferFrameFactory, ITreeMetaFrameFactory metaFrameFactory, ITupleWriter writer, ITupleComparator comparator, IPrimaryBufferManager bufferManager, IBloomFilterFactory bloomFilterFactory, boolean needsDupKeyCheck, int recordSize) {
        super(bufferCache, fileMapProvider, freePageManager, file, rootNodeFrameFactory, nodeFrameFactory, leafFrameFactory, bufferFrameFactory, metaFrameFactory, writer, comparator, bufferManager, needsDupKeyCheck, recordSize);
        this.bloomFilterFactory = bloomFilterFactory;
    }

    /**
     * This function is invoked to create the first ever leaf on which the buffer is emptied.
     * @param ctx
     * @throws HyracksDataException
     */
    protected void createFirstLeaf(EmptyBufferOpContext ctx) throws HyracksDataException {
        //get a page id from freepage manager, get a new page from buffer cache, and initiate the frame.
        int newLeafPageId = freePageManager.getFreePage(metaFrame);
        ICachedPage newLeafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newLeafPageId), true);
        newLeafPage.acquireWriteLatch();
        ctx.writeLatchCount++;
        ctx.leafFrame.setPage(newLeafPage);
        ctx.leafFrame.initBuffer((byte)0);
        newLeafPage.releaseWriteLatch(true);
        ctx.writeLatchCount--;
        bufferCache.unpin(newLeafPage);

        //create and insert an index tuple for the newly created leaf
        IIndexTupleWithBloomFilter newLeafTuple = writer.createNewIndexTupleWithBF();
        newLeafTuple.setKey(KeyValueTuple.LOWEST_KEY_TUPLE);
        newLeafTuple.setPageId(newLeafPageId);
        if (ctx.rootNodeFrame.insertIntoNode(newLeafTuple) != FrameOpStatus.COMPLETED)
            throw new AssertionError();

        metaFrame.getPage().acquireWriteLatch();
        ctx.writeLatchCount++;
        metaFrame.incrementHeight();
        metaFrame.incrementHeight();
        metaFrame.getPage().releaseWriteLatch(true);
        ctx.writeLatchCount--;
    }

    protected void search(IIndexCursor cursor, ISearchPredicate predicate) throws HyracksDataException, IndexException {

        PointSearchCursor lookupCursor = (PointSearchCursor) cursor;
        PointSearchPredicate pred = (PointSearchPredicate)predicate;
        lookupCursor.open(null, pred);

        ITupleReference temp;
        temp = bufferManager.getMatchingKeyTuple(pred.searchKey);
        if(temp != null) {
            lookupCursor.matchingKeyValue = writer.createNewTuple();
            lookupCursor.matchingKeyValue.deepCopy(temp);
            return;
        }

        int level = metaFrame.getHeight() - 1;
        if(level <= 0) {
            return;
        }

        //obtain the child index from root node
        int childPageId = -1;
        boolean cont = true;
        ITreeNodeFrame rootNodeFrame = rootNodeFrameFactory.createFrame();
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPageId), false);
        page.acquireReadLatch();
        rootNodeFrame.setPage(page);
        level = rootNodeFrame.getLevel();
        if(level == 1) {
            IIndexTupleReference indexTuple = rootNodeFrame.getChildIndexTuple(pred.searchKey);
            IIndexTupleWithBloomFilter indexTupleWithBloomFilter = new KeyValueIndexTupleWithBloomFilter();
            indexTupleWithBloomFilter.deepCopy(indexTuple);
            BloomFilter bf = indexTupleWithBloomFilter.getBloomFilter();
            if(!bf.mightContain(((KeyValueTuple)pred.searchKey).getKey())) {
                cont = false;
            } else {
                childPageId = indexTuple.getPageId();
            }
        } else {
            childPageId = rootNodeFrame.getChildPageId(pred.searchKey);
        }
        level--;
        page.releaseReadLatch();
        bufferCache.unpin(page);

        while(level >= 0 && cont) {
            page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, childPageId), false);
            page.acquireReadLatch();
            if(level >= 1) {
                //get the child page id from the node
                ITreeNodeWithBufferFrame nodeWithBufferFrame = nodeFrameFactory.createFrame(level);
                nodeWithBufferFrame.setPage(page);
                //check in the buffer
                temp = nodeWithBufferFrame.getMatchingKeyInBuffer(pred.searchKey);
                if(temp != null) {
                    lookupCursor.matchingKeyValue = writer.createNewTuple();
                    lookupCursor.matchingKeyValue.deepCopy(temp);
                    cont = false;
                } else {
                    //if leaf node then check if the element exists using the bloomfilter.
                    if(level == 1) {
                        IIndexTupleReference indexTuple = nodeWithBufferFrame.getChildIndexTuple(pred.searchKey);
                        IIndexTupleWithBloomFilter indexTupleWithBloomFilter = new KeyValueIndexTupleWithBloomFilter();
                        indexTupleWithBloomFilter.deepCopy(indexTuple);
                        if ((indexTupleWithBloomFilter.getFieldLength(2) <= 150)) throw new AssertionError();
                        BloomFilter bf = indexTupleWithBloomFilter.getBloomFilter();
                        if(!bf.mightContain(((KeyValueTuple)pred.searchKey).getKey())) {
                            cont = false;
                        } else {
                            childPageId = indexTuple.getPageId();
                        }
                    } else {
                        childPageId = nodeWithBufferFrame.getChildPageId(pred.searchKey);
                    }
                }
            } else {
                ITreeLeafFrame leafFrame = leafFrameFactory.createFrame();
                leafFrame.setPage(page);

                //search in the leaf now
                temp = leafFrame.getMatchingKeyTuple(pred.searchKey);
                if(temp != null) {
                    lookupCursor.matchingKeyValue = writer.createNewTuple();
                    lookupCursor.matchingKeyValue.deepCopy(temp);
                    cont = false;
                }
            }

            page.releaseReadLatch();
            bufferCache.unpin(page);
            level--;
        }

    }

    /**
     * Empties the buffer of node at pageId and returns the new children that are created in the children array list
     * @param ctx
     * @param pageId
     * @param children
     * @throws HyracksDataException
     */
    protected void emptyBuffer(EmptyBufferOpContext ctx, int pageId, ArrayList<IIndexTupleReference> children, boolean isLeafNode) throws HyracksDataException {

        IIndexTupleWithBloomFilter indexTuple = writer.createNewIndexTupleWithBF();
        IReusableTupleReference endTuple = writer.createNewTuple();
        int childOffset = -1;

        //traverse the children nodes/leafs, limit the bufferIterator and invoke insertLeaf or insertBuffer
        while(!ctx.childrenIterator.done() && !ctx.bufferIterator.done()) {

            //retrieve child page id from indexTuple
            indexTuple.reset(ctx.childrenIterator.peek());
            childOffset = ctx.childrenIterator.getPeekOffset();
            ctx.childrenIterator.next();
            int childPageId = indexTuple.getPageId();

            //limit the bufferIterator based on the nextIndex index tuple
            if(ctx.childrenIterator.hasNext()) {
                endTuple.reset(ctx.childrenIterator.peek());
            } else {
                //reset with highest tuple as this is the last child
                endTuple.reset(KeyValueTuple.HIGHEST_KEY_TUPLE);
            }
            ctx.bufferIterator.setEndLimit(endTuple);

            //invoke the insertBuffer or insertLeaf accordingly.
            if(ctx.bufferIterator.hasNext()) {
                if(isLeafNode) {
                    ctx.bfIndexTuple = indexTuple;
                    insertLeaf(ctx, childPageId, children);
                    ctx.childrenIterator.unsafeUpdate(ctx.bfIndexTuple, childOffset);
                } else {
                    insertBuffer(ctx, childPageId, children);
                }
            }

            //all elements from the bufferIterator have to be emptied
            if (ctx.bufferIterator.hasNext()) {
                throw new AssertionError();
            }
        }

        //all elements from the bufferIterator have to be emptied
        if (!ctx.bufferIterator.done()) {
            throw new AssertionError();
        }
    }
    /**
     * Inserts the elements in ctx.bufferIterator into the leaf at pageId. It splits the leaf, if neccessary into how many ever
     * leafs it wants and appends the siblings into the array list
     * @param ctx
     * @param pageId
     * @param siblings
     * @throws HyracksDataException
     */
    protected void insertLeaf(EmptyBufferOpContext ctx, int pageId, ArrayList<IIndexTupleReference> siblings) throws HyracksDataException {

        //obtain the leaf page from buffer cache and set it into the leaf frame
        ICachedPage leafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        ctx.leafFrame.setPage(leafPage);
        ctx.leafFrame.resetLeafState();

        leafPage.acquireWriteLatch();
        ctx.writeLatchCount++;
        try {

            //invoke insertIntoLeaf on the leaf frame with the bufferIterator
            FrameOpStatus opStatus = ctx.leafFrame.insertIntoLeaf(ctx.bufferIterator);

            //if not completed, split the leaf!
            if(opStatus != FrameOpStatus.COMPLETED) {

                //calculate the splitSize and numLeafs
                int numLeafs, splitSize, totalSize;
                totalSize = ctx.leafFrame.getLeafDataSize() + ctx.bufferIterator.estimateSize();
                numLeafs = (int) Math.ceil(1.25 * totalSize / (double)ctx.leafFrame.getLeafCapacity());
                splitSize = totalSize/numLeafs;

                ArrayList<IIndexTupleWithBloomFilter> splitKeys = new ArrayList<IIndexTupleWithBloomFilter>();
                ArrayList<ITreeLeafFrameWithBloomFilter> leafFrames = new ArrayList<ITreeLeafFrameWithBloomFilter>();

                //copy the contents of current leaf Frame page onto a temporary page and
                //empty the current page
                leafPage = ctx.leafFrame.getPage();
                ctx.tempCachedPage.deepCopy(leafPage);
                ctx.leafFrame.setPage(ctx.tempCachedPage);

                ITreeLeafFrameWithBloomFilter oldLeafFrame = (ITreeLeafFrameWithBloomFilter)leafFrameFactory.createFrame();
                oldLeafFrame.setPage(leafPage);
                oldLeafFrame.resetLeafSpaceParams();

                leafFrames.add(oldLeafFrame);

                try {

                    //the correspondence relation between pageIds, splitKeys and iterators are as follows:
                    // newLeafPageIds[id] <-> leafFrames[id] <-> (index tuple as splitKeys[id])
                    int[] newLeafPageIds = new int[numLeafs-1];
                    for(int i = 0; i < numLeafs - 1; i++) {
                        //initiate a new leaf frame
                        ITreeLeafFrameWithBloomFilter newLeafFrame = (ITreeLeafFrameWithBloomFilter)leafFrameFactory.createFrame();
                        int newLeafPageId = freePageManager.getFreePage(metaFrame);

                        newLeafPageIds[i] = newLeafPageId;
                        ICachedPage newLeafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newLeafPageId), true);
                        newLeafPage.acquireWriteLatch();
                        ctx.writeLatchCount++;

                        newLeafFrame.setPage(newLeafPage);
                        newLeafFrame.initBuffer((byte)0);
                        leafFrames.add(newLeafFrame);

                        //location to store the split key
                        IIndexTupleWithBloomFilter splitKey = writer.createNewIndexTupleWithBF();
                        splitKeys.add(splitKey);
                    }

                    //invoke distribute function on ctx.leafFrame
                    ctx.leafFrame.distributeAndInsert(splitSize, ctx.bufferIterator, leafFrames, splitKeys);


                    ctx.bfIndexTuple.setBloomFilter(oldLeafFrame.getBloomFilter());
                    for(int i = 0; i < numLeafs - 1; i++) {
                        splitKeys.get(i).setPageId(newLeafPageIds[i]);
                        splitKeys.get(i).setBloomFilter(leafFrames.get(i+1).getBloomFilter());
                    }

                    //important step: add all the siblings to the array list
                    siblings.addAll(splitKeys);

                } finally {
                    //release the write latches for new leaf iterators
                    for(int i = 1; i < leafFrames.size(); i++) {
                        leafFrames.get(i).getPage().releaseWriteLatch(true);
                        ctx.writeLatchCount--;
                        bufferCache.unpin(leafFrames.get(i).getPage());
                    }
                }

                if(ctx.bufferIterator.hasNext()) {
                    throw new AssertionError();
                }

            } else {
                ctx.bfIndexTuple.addBloomFilter(((ITreeLeafFrameWithBloomFilter) ctx.leafFrame).getBloomFilter());
            }
        } finally {
            leafPage.releaseWriteLatch(true);
            ctx.writeLatchCount--;
            bufferCache.unpin(leafPage);

            //all elements from the bufferIterator have to be emptied
            if (ctx.bufferIterator.hasNext()) {
                throw new AssertionError();
            }
        }

    }

    public boolean hasBloomFilters() {
        return true;
    }
}
