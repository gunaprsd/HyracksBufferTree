package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IIndexTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.common.IReusableTupleReference;
import org.apache.hyracks.storage.am.buffertree.api.frames.*;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueIndexTuple;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTuple;
import org.apache.hyracks.storage.am.common.api.*;
import org.apache.hyracks.storage.common.buffercache.*;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

import java.io.PrintStream;
import java.util.ArrayList;

public class BufferTree implements IIndex {

    public static final int metaPageId = 0;
    public static final int rootPageId = 1;
    public final FileReference file;
    public final IBufferCache bufferCache;
    public final IFileMapProvider fileMapProvider;
    public final IFreePageManager freePageManager;
    public final IPrimaryBufferManager bufferManager;
    public final ITreeLeafFrameFactory leafFrameFactory;
    public final ITreeNodeWithBufferFrameFactory nodeFrameFactory;
    public final ITreeNodeFrameFactory rootNodeFrameFactory;
    public final ITreeMetaFrameFactory metaFrameFactory;
    public final IPrimaryBufferFrameFactory bufferFrameFactory;
    public final ITupleComparator comparator;
    public final ITupleWriter writer;

    public int fileId;
    public int recordSize;
    public ITreeMetaFrame metaFrame;
    protected boolean needsDupKeyCheck;
    protected boolean isActivated;

    public BufferTree(IBufferCache bufferCache, IFileMapProvider fileMapProvider, IFreePageManager freePageManager, FileReference file,
                      ITreeNodeFrameFactory rootNodeFrameFactory, ITreeNodeWithBufferFrameFactory nodeFrameFactory,
                      ITreeLeafFrameFactory leafFrameFactory, IPrimaryBufferFrameFactory bufferFrameFactory,
                      ITreeMetaFrameFactory metaFrameFactory, ITupleWriter writer,
                      ITupleComparator comparator, IPrimaryBufferManager bufferManager, boolean needsDupKeyCheck, int recordSize) {
        this.file = file;
        this.bufferCache = bufferCache;
        this.bufferManager = bufferManager;
        this.fileMapProvider = fileMapProvider;
        this.freePageManager = freePageManager;
        this.leafFrameFactory = leafFrameFactory;
        this.nodeFrameFactory = nodeFrameFactory;
        this.rootNodeFrameFactory = rootNodeFrameFactory;
        this.metaFrameFactory = metaFrameFactory;
        this.bufferFrameFactory = bufferFrameFactory;
        this.comparator = comparator;
        this.writer = writer;
        this.isActivated = false;
        this.bufferManager.initialize(this);
        this.needsDupKeyCheck = needsDupKeyCheck;
        this.recordSize = recordSize;
    }

    public synchronized void create() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }

        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(file);
            if (!fileIsMapped) {
                bufferCache.createFile(file);
            }
            fileId = fileMapProvider.lookupFileId(file);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }

        freePageManager.open(fileId);
        initEmptyTree();
        freePageManager.close();
        bufferCache.closeFile(fileId);
    }

    protected synchronized void initEmptyTree() throws HyracksDataException {
        //initiate the meta page
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metaPageId), true);
        metaPage.acquireWriteLatch();
        metaFrame = (ITreeMetaFrame) freePageManager.getMetaDataFrameFactory().createFrame();
        metaFrame.setPage(metaPage);
        metaFrame.initBuffer(ITreeMetaFrame.META_PAGE_LEVEL);
        metaPage.releaseWriteLatch(true);
        bufferCache.unpin(metaPage);


        //initiate the free page manager
        freePageManager.init(metaFrame, 5);

        //initiate the root node page
        ITreeNodeFrame rootNodeFrame = rootNodeFrameFactory.createFrame();
        ICachedPage rootPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPageId), true);
        rootPage.acquireWriteLatch();
        rootNodeFrame.setPage(rootPage);
        rootNodeFrame.initBuffer((byte)1);
        rootPage.releaseWriteLatch(true);
        bufferCache.unpin(rootPage);

        //this sets the primary buffer page
    }

    public synchronized void activate() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }

        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(file);
            if (!fileIsMapped) {
                bufferCache.createFile(file);
            }
            fileId = fileMapProvider.lookupFileId(file);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }

        //opening meta frame
        metaFrame = metaFrameFactory.createFrame();
        ICachedPage metaPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metaPageId), false);
        metaFrame.setPage(metaPage);

        //initiating buffer manager
        bufferManager.open();
        freePageManager.open(fileId);

        isActivated = true;
    }

    public synchronized void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        initEmptyTree();
    }

    public synchronized void deactivate() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }

        bufferManager.close();
        bufferCache.closeFile(fileId);
        freePageManager.close();
        isActivated = false;
    }

    public synchronized void destroy() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }

        if (fileId == -1) {
            return;
        }
        bufferCache.deleteFile(fileId, false);
        file.delete();
        fileId = -1;
    }

    public IIndexAccessor createAccessor(IModificationOperationCallback iModificationOperationCallback, ISearchOperationCallback iSearchOperationCallback) throws HyracksDataException {
        return new BufferTreeIndexAccessor(this);
    }

    public void validate() throws HyracksDataException {
        //TODO implmement a function to validate the file: for future!
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public long getMemoryAllocationSize() {
        return 0;
    }

    public IIndexBulkLoader createBulkLoader(float v, boolean b, long l, boolean b1) throws IndexException {
        return null;
    }

    public boolean hasMemoryComponents() {
        return false;
    }

    protected void insert(ITupleReference tuple) throws HyracksDataException {
        bufferManager.insert(tuple);
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
        int childPageId;
        ITreeNodeFrame rootNodeFrame = rootNodeFrameFactory.createFrame();
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPageId), false);
        page.acquireReadLatch();
        rootNodeFrame.setPage(page);
        childPageId = rootNodeFrame.getChildPageId(pred.searchKey);
        level--;
        page.releaseReadLatch();
        bufferCache.unpin(page);

        boolean cont = true;
        while(level >= 0 && cont) {
            page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, childPageId), false);
            page.acquireReadLatch();
            if(level > 0) {
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
                    //if not present in node buffer, then go to child
                    childPageId = nodeWithBufferFrame.getChildPageId(pred.searchKey);
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
        IIndexTupleReference newLeafTuple = writer.createNewIndexTuple();
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

    /**
     * Function invoked by primary buffer manager. primaryBufferIterator could contain more than one primary buffer. It presents
     * a merged view of all the iterators. Invokes emptyBuffer function appropriately. Contains a read latch on the root node page through
     * the entire course of emptying.
     * @param ctx
     * @param primaryBufferIterator
     * @throws HyracksDataException
     */
    public void emptyPrimaryBuffer(EmptyBufferOpContext ctx, IOrderedTupleIterator primaryBufferIterator) throws HyracksDataException{

        ArrayList<IIndexTupleReference> children = new ArrayList<IIndexTupleReference>();
        ICachedPage rootPage = null;
        boolean isReadLatched = false;
        boolean isWriteLatched = false;
        try {
            rootPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPageId), false);
            rootPage.acquireReadLatch();
            ctx.readLatchCount++;
            isReadLatched = true;
            ctx.rootNodeFrame.setPage(rootPage);
            ctx.rootNodeFrame.resetNodeState();

            //If children count = 0, then we need to initiate with a new leaf inserted.
            if(ctx.rootNodeFrame.getChildrenCount() == 0) {
                rootPage.releaseReadLatch();
                ctx.readLatchCount--;
                isReadLatched = false;
                rootPage.acquireWriteLatch();
                ctx.writeLatchCount++;
                isWriteLatched = true;
                try {
                    createFirstLeaf(ctx);
                } finally {
                    rootPage.releaseWriteLatch(true);
                    ctx.writeLatchCount--;
                    isWriteLatched = false;
                    ctx.rootNodeFrame.getPage().acquireReadLatch();
                    ctx.readLatchCount++;
                    isReadLatched = true;
                }
            }

            ctx.bufferIterator = primaryBufferIterator;
            ctx.childrenIterator = ctx.rootNodeFrame.getChildrenIterator();
            emptyBuffer(ctx, rootPageId, children, ctx.rootNodeFrame.isLeafNode());

            if(children.size() > 0) {
                rootPage.releaseReadLatch();
                ctx.readLatchCount--;
                isReadLatched = false;
                rootPage.acquireWriteLatch();
                ctx.writeLatchCount++;
                isWriteLatched = true;
                try {
                    insertIntoRootNode(ctx, children);
                } finally {
                    rootPage.releaseWriteLatch(true);
                    ctx.writeLatchCount--;
                    isWriteLatched = false;
                }
            } else {
                rootPage.releaseReadLatch();
                ctx.readLatchCount--;
                isReadLatched = false;
            }
        } catch (HyracksDataException e) {
            e.printStackTrace();
        } finally {
            if(isReadLatched) {
                rootPage.releaseReadLatch();
                ctx.readLatchCount--;
            }
            else if(isWriteLatched) {
                rootPage.releaseWriteLatch(true);
                ctx.writeLatchCount--;
            }

            bufferCache.unpin(rootPage);
        }

        if (ctx.readLatchCount != 0) throw new AssertionError();
        if (ctx.writeLatchCount != 0) throw new AssertionError();
    }

    /**
     * Empties the buffer of node at pageId and returns the new children that are created in the children array list
     * @param ctx
     * @param pageId
     * @param children
     * @throws HyracksDataException
     */
    protected void emptyBuffer(EmptyBufferOpContext ctx, int pageId, ArrayList<IIndexTupleReference> children, boolean isLeafNode) throws HyracksDataException /**/{

        IIndexTupleReference indexTuple = writer.createNewIndexTuple();
        IReusableTupleReference endTuple = writer.createNewTuple();

        //traverse the children nodes/leafs, limit the bufferIterator and invoke insertLeaf or insertBuffer
        while(!ctx.childrenIterator.done() && !ctx.bufferIterator.done()) {

            //retrieve child page id from indexTuple
            indexTuple.reset(ctx.childrenIterator.peek());
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
                    insertLeaf(ctx, childPageId, children);
                } else {
                    insertBuffer(ctx, childPageId, children);
                }
            }
        }

        //all elements from the bufferIterator have to be emptied
        if (!ctx.bufferIterator.done()) {
            throw new AssertionError();
        }
    }

    /**
     * Insert the ctx.bufferIterator items into the buffer. if insert failed: empty the buffer and insert. Empty the buffer multiple
     * times if one emptying does not create space for all elements in the bufferIterator. It also invokes insertIntoNode,
     * if new children were created due to emptying buffer.
     * @param ctx
     * @param pageId
     * @param siblings
     * @throws HyracksDataException
     */
    protected void insertBuffer(EmptyBufferOpContext ctx, int pageId, ArrayList<IIndexTupleReference> siblings) throws HyracksDataException {
        //obtain the node page from the buffer cache, pop the top node with buffer frame and set the given page into the frame
        ICachedPage nodePage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        ITreeNodeWithBufferFrame currentFrame = ctx.interiorNodeFrames.peek();
        currentFrame.setPage(nodePage);

        nodePage.acquireWriteLatch();
        ctx.writeLatchCount++;

        try {
            //try inserting the elements in the bufferIterator into the buffer
            FrameOpStatus opStatus = currentFrame.insertIntoBuffer(ctx.bufferIterator);

            if(opStatus != FrameOpStatus.COMPLETED) {

                //used to record the current end limit to be used in case there is a split of node
                IReusableTupleReference lastEndTuple = writer.createNewTuple();
                lastEndTuple.reset(ctx.bufferIterator.getCurrentLimit());

                //newSiblings records the total number of siblings created on invocation of this function
                ArrayList<IIndexTupleReference> newSiblings = new ArrayList<IIndexTupleReference>();
                //tempSiblings represents the number of siblings created on a single loop : multiple empty buffer pains!
                ArrayList<IIndexTupleReference> tempSiblings = new ArrayList<IIndexTupleReference>();
                //Used to record children created during each loop iteration
                ArrayList<IIndexTupleReference> children = new ArrayList<IIndexTupleReference>();

                //store state before getting into the loop
                boolean isLeafNode  = currentFrame.isLeafNode();
                IOrderedTupleIterator parentBufferIterator = ctx.bufferIterator;
                IOrderedTupleIterator parentChildrenIterator = ctx.childrenIterator;

                while(opStatus != FrameOpStatus.COMPLETED) {
                    //you need to get the iterator freshly again on every loop as the children and buffer elements have changed
                    children.clear();
                    tempSiblings.clear();
                    ctx.childrenIterator = currentFrame.getChildrenIterator();
                    ctx.bufferIterator = currentFrame.getBufferIterator();

                    //invoke emptyBuffer function
                    ctx.interiorNodeFrames.pop();
                    emptyBuffer(ctx, pageId, children, isLeafNode);
                    ctx.interiorNodeFrames.push(currentFrame);

                    //change the space parameters in the buffer to denote it is empty
                    currentFrame.resetBufferSpaceParams();

                    //also reset the state variables
                    currentFrame.resetBufferState();


                    //if any new children were created, invoke the insertIntoNode function - that takes care of splitting if any
                    if(children.size() > 0) {

                        insertIntoNode(ctx, pageId, children, tempSiblings);

                        //check if new siblings are there and set an appropriate bound
                        if(tempSiblings.size() > 0) {
                            newSiblings.addAll(tempSiblings);
                            parentBufferIterator.setEndLimit(tempSiblings.get(0));
                        }
                    }

                    //insert the rest of the elements in the bufferIterator into the buffer.
                    //please note: this might be wrong as there could be splitting of nodes when we insert the
                    //new children created. But splitting takes care of splitting the buffer too! ;)
                    opStatus = currentFrame.insertIntoBuffer(parentBufferIterator);
                }
                ctx.bufferIterator = parentBufferIterator;
                ctx.childrenIterator = parentChildrenIterator;



                //insert the rest of the elements into other buffers
                for(int i = 0; i < newSiblings.size(); i++) {
                    tempSiblings.clear();

                    int siblingPageId = newSiblings.get(i).getPageId();
                    if(i < newSiblings.size() - 1) {
                        ctx.bufferIterator.setEndLimit(newSiblings.get(i + 1));
                    } else {
                        ctx.bufferIterator.setEndLimit(lastEndTuple);
                    }

                    insertBuffer(ctx, siblingPageId, tempSiblings);
                    newSiblings.addAll(tempSiblings);
                }

                siblings.addAll(newSiblings);

            }

        } finally {
            nodePage.releaseWriteLatch(true);
            ctx.writeLatchCount--;
            bufferCache.unpin(nodePage);
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
                //numLeafs = (int)Math.ceil(totalSize/(splitSize - recordSize));

                ArrayList<IIndexTupleReference> splitKeys = new ArrayList<IIndexTupleReference>();
                ArrayList<ITreeLeafFrame> leafFrames = new ArrayList<ITreeLeafFrame>();

                //copy the contents of current leaf Frame page onto a temporary page and
                //empty the current page
                leafPage = ctx.leafFrame.getPage();
                ctx.tempCachedPage.deepCopy(leafPage);
                ctx.leafFrame.setPage(ctx.tempCachedPage);

                ITreeLeafFrame oldLeafFrame = leafFrameFactory.createFrame();
                oldLeafFrame.setPage(leafPage);
                oldLeafFrame.resetLeafSpaceParams();

                leafFrames.add(oldLeafFrame);

                try {

                    //the correspondence relation between pageIds, splitKeys and iterators are as follows:
                    // newLeafPageIds[id] <-> leafFrames[id] <-> (index tuple as splitKeys[id])
                    int[] newLeafPageIds = new int[numLeafs-1];
                    for(int i = 0; i < numLeafs - 1; i++) {
                        //initiate a new leaf frame
                        ITreeLeafFrame newLeafFrame = leafFrameFactory.createFrame();
                        int newLeafPageId = freePageManager.getFreePage(metaFrame);

                        newLeafPageIds[i] = newLeafPageId;
                        ICachedPage newLeafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newLeafPageId), true);
                        newLeafPage.acquireWriteLatch();
                        ctx.writeLatchCount++;

                        newLeafFrame.setPage(newLeafPage);
                        newLeafFrame.initBuffer((byte)0);
                        leafFrames.add(newLeafFrame);

                        //location to store the split key
                        IIndexTupleReference splitKey = writer.createNewIndexTuple();
                        splitKeys.add(splitKey);
                    }

                    //invoke distribute function on ctx.leafFrame
                    ctx.leafFrame.distributeAndInsert(splitSize, ctx.bufferIterator, leafFrames, splitKeys);


                    for(int i = 0; i < numLeafs - 1; i++) {
                        splitKeys.get(i).setPageId(newLeafPageIds[i]);
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

                if(ctx.bufferIterator.hasNext()) throw new AssertionError();
            }

        } finally {
            leafPage.releaseWriteLatch(true);
            ctx.writeLatchCount--;
            bufferCache.unpin(leafPage);
        }

    }

    /**
     * Functions inserts (splits if necessary) the children into the given node. It adds the newly created siblings, if any, in the siblings
     * array list. The frame for the current node must already be set as this function is called only after empty buffer function.
     * Important: the node might contain buffer elements which are inserted after the buffer empty process.
     * @param ctx
     * @param pageId
     * @param children
     * @param siblings
     * @throws HyracksDataException
     */
    protected void insertIntoNode(EmptyBufferOpContext ctx, int pageId, ArrayList<IIndexTupleReference> children, ArrayList<IIndexTupleReference> siblings) throws HyracksDataException {

        ITreeNodeWithBufferFrame currentFrame = ctx.interiorNodeFrames.peek();

        FrameOpStatus opStatus = FrameOpStatus.COMPLETED;
        int childIndex = 0;
        for(; childIndex < children.size(); childIndex++) {
            opStatus = currentFrame.insertIntoNode(children.get(childIndex));
            if(opStatus != FrameOpStatus.COMPLETED) {
                //no space in the node. break and deal with it outside
                break;
            }
        }

        if(opStatus != FrameOpStatus.COMPLETED) {
            IOrderedTupleIterator childrenIterator = new ArrayListIterator(children, childIndex);

            //calculate the splitSize and numNodes
            int numNodes, splitSize, totalSize;
            totalSize = currentFrame.getNodeDataSize() + childrenIterator.estimateSize();
            numNodes = (int) Math.ceil(1.25 * totalSize / (double)currentFrame.getNodeCapacity());
            splitSize = totalSize/numNodes;


            ArrayList<IIndexTupleReference> splitKeys = new ArrayList<IIndexTupleReference>();
            ArrayList<ITreeNodeFrame> nodeFrames = new ArrayList<ITreeNodeFrame>();

            //copy the contents of current node Frame page onto a temporary page and
            //empty the current page
            ICachedPage nodePage = currentFrame.getPage();
            ctx.tempCachedPage.deepCopy(nodePage);
            currentFrame.setPage(ctx.tempCachedPage);

            ITreeNodeFrame oldNodeFrame = nodeFrameFactory.createFrame(currentFrame.getLevel());
            oldNodeFrame.setPage(nodePage);
            oldNodeFrame.resetNodeSpaceParams();

            nodeFrames.add(oldNodeFrame);

            try {
                //the correspondence relation between pageIds, splitKeys and iterators are as follows:
                // newNodePageIds[id] <-> nodeFrames[id] <-> (index tuple as splitKeys[id])
                int[] newNodePageIds = new int[numNodes -1];
                for(int i = 0; i < numNodes - 1; i++) {
                    //initiate a new leaf frame
                    ITreeNodeFrame newNodeFrame = nodeFrameFactory.createFrame(currentFrame.getLevel());
                    int newNodePageId = freePageManager.getFreePage(metaFrame);

                    newNodePageIds[i] = newNodePageId;
                    ICachedPage newNodePage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newNodePageId), true);
                    newNodePage.acquireWriteLatch();
                    ctx.writeLatchCount++;

                    newNodeFrame.setPage(newNodePage);
                    newNodeFrame.initBuffer(currentFrame.getLevel());
                    nodeFrames.add(newNodeFrame);

                    //location to store the split key
                    IIndexTupleReference splitKey = writer.createNewIndexTuple();
                    splitKeys.add(splitKey);
                }

                //invoke distribute function on currentFrame
                currentFrame.distributeNodeAndInsert(splitSize, childrenIterator, nodeFrames, splitKeys);


                for(int i = 0; i < numNodes - 1; i++) {
                    splitKeys.get(i).setPageId(newNodePageIds[i]);
                }

                //important step: add all the siblings to the array list
                siblings.addAll(splitKeys);

            } finally {
                //restore the current frame
                currentFrame.setPage(nodeFrames.get(0).getPage());

                //release the write latches for new leaf iterators
                for(int i = 1; i < nodeFrames.size(); i++) {
                    nodeFrames.get(i).getPage().releaseWriteLatch(true);
                    ctx.writeLatchCount--;
                    bufferCache.unpin(nodeFrames.get(i).getPage());
                }
            }

            if(childrenIterator.hasNext()) throw new AssertionError();

        } else {
            if (childIndex != children.size()) throw new AssertionError();
        }
    }

    /**
     * Special status for root node it is a node frame without any buffer. Splitting is complicated because it has to distributed
     * into nodes with buffer which means we may need more than two nodes.
     * @param ctx
     * @param children
     * @throws HyracksDataException
     */
    protected void insertIntoRootNode(EmptyBufferOpContext ctx, ArrayList<IIndexTupleReference> children) throws HyracksDataException {

        FrameOpStatus opStatus = FrameOpStatus.COMPLETED;
        int childIndex = 0;
        for(; childIndex < children.size(); childIndex++) {
            opStatus = ctx.rootNodeFrame.insertIntoNode(children.get(childIndex));
            if(opStatus != FrameOpStatus.COMPLETED) {
                //no space in the node. break and deal with it outside
                break;
            }
        }

        if(opStatus != FrameOpStatus.COMPLETED) {
            IOrderedTupleIterator childrenIterator = new ArrayListIterator(children, childIndex);

            ArrayList<IIndexTupleReference> siblings = new ArrayList<IIndexTupleReference>();

            //calculate the splitSize and numNodes
            int numNodes, splitSize, totalSize;
            totalSize = ctx.rootNodeFrame.getNodeDataSize() + childrenIterator.estimateSize();
            numNodes = (int) Math.ceil(1.25 * totalSize / (double)ctx.rootNodeFrame.getNodeCapacity());
            splitSize = totalSize/numNodes;


            ArrayList<IIndexTupleReference> splitKeys = new ArrayList<IIndexTupleReference>();
            ArrayList<ITreeNodeFrame> nodeFrames = new ArrayList<ITreeNodeFrame>();

            try {
                //the correspondence relation between pageIds, splitKeys and iterators are as follows:
                // newNodePageIds[id] <-> nodeFrames[id] <-> (index tuple as splitKeys[id])
                int[] newNodePageIds = new int[numNodes];
                for(int i = 0; i < numNodes; i++) {
                    //initiate a new leaf frame
                    ITreeNodeFrame newNodeFrame = nodeFrameFactory.createFrame(ctx.rootNodeFrame.getLevel());
                    int newNodePageId = freePageManager.getFreePage(metaFrame);

                    newNodePageIds[i] = newNodePageId;
                    ICachedPage newNodePage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newNodePageId), true);
                    newNodePage.acquireWriteLatch();
                    ctx.writeLatchCount++;

                    newNodeFrame.setPage(newNodePage);
                    newNodeFrame.initBuffer(ctx.rootNodeFrame.getLevel());
                    nodeFrames.add(newNodeFrame);

                    //location to store the split key
                    if(i < numNodes - 1) {
                        IIndexTupleReference splitKey = writer.createNewIndexTuple();
                        splitKeys.add(splitKey);
                    }
                }

                //invoke distribute function on currentFrame
                ctx.rootNodeFrame.distributeNodeAndInsert(splitSize, childrenIterator, nodeFrames, splitKeys);


                IIndexTupleReference firstTuple = writer.createNewIndexTuple();
                firstTuple.setKey(KeyValueIndexTuple.LOWEST_KEY_TUPLE);
                firstTuple.setPageId(newNodePageIds[0]);
                splitKeys.add(0, firstTuple);

                for(int i = 1; i < numNodes; i++) {
                    splitKeys.get(i).setPageId(newNodePageIds[i]);
                }

                //important step: add all the siblings to the array list
                siblings.addAll(splitKeys);

            } finally {
                //release the write latches for new leaf iterators
                for(int i = 0; i < nodeFrames.size(); i++) {
                    nodeFrames.get(i).getPage().releaseWriteLatch(true);
                    ctx.writeLatchCount--;
                    bufferCache.unpin(nodeFrames.get(i).getPage());
                }
            }

            if(childrenIterator.hasNext()) throw new AssertionError();

            createNewRoot(ctx, siblings);

        } else {
            if (childIndex != children.size()) throw new AssertionError();
        }
    }

    /**
     * Cleans up root node page and inserts the children. Write locks already held!
     * @param ctx
     * @param children
     * @throws HyracksDataException
     */
    public void createNewRoot(EmptyBufferOpContext ctx, ArrayList<IIndexTupleReference> children) throws HyracksDataException {
        //clean up the root node
        ctx.rootNodeFrame.initBuffer((byte)metaFrame.getHeight());
        ctx.rootNodeFrame.resetNodeSpaceParams();
        ctx.rootNodeFrame.resetNodeState();

        //inserting the newly created children into root node.
        for(int j = 0; j < children.size(); j++) {
            if (ctx.rootNodeFrame.insertIntoNode(children.get(j)) != FrameOpStatus.COMPLETED)
                throw new AssertionError();
        }

        //increment the height
        metaFrame.getPage().acquireWriteLatch();
        ctx.writeLatchCount++;
        metaFrame.incrementHeight();
        metaFrame.getPage().releaseWriteLatch(true);
        ctx.writeLatchCount--;
    }

    public void printLeaf(int pageId, PrintStream out) throws HyracksDataException {
        ITreeLeafFrame leafFrame = leafFrameFactory.createFrame();
        ICachedPage leafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        leafPage.acquireReadLatch();
        leafFrame.setPage(leafPage);

        IOrderedTupleIterator childrenIterator = leafFrame.getLeafIterator();
        out.format("<leaf>\n");
        out.format("<meta>\n");
        out.format("Page id : %d\n", pageId);
        out.format("Free Space : %d\n", leafFrame.getLeafFreeSpace());
        out.format("</meta>\n");
        out.format("<children>\n");
        int i = 0;
        while(childrenIterator.hasNext()) {
            out.format("Item %d : %s\n", i, childrenIterator.peek());
            childrenIterator.next();
            i++;
        }
        out.format("</children>\n");
        out.format("</leaf>\n\n");
        leafPage.releaseReadLatch();
        bufferCache.unpin(leafPage);
    }

    public void printNode(int level, int pageId, PrintStream out) throws HyracksDataException {
        ITreeNodeWithBufferFrame nodeWithBufferFrame = nodeFrameFactory.createFrame(level);
        ICachedPage nodePage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        nodePage.acquireReadLatch();
        nodeWithBufferFrame.setPage(nodePage);

        int[] pageIds = new int[nodeWithBufferFrame.getChildrenCount()];
        IIndexTupleReference indexTuple = writer.createNewIndexTuple();
        IOrderedTupleIterator iterator = nodeWithBufferFrame.getChildrenIterator();
        out.format("<node>\n");
        out.format("<meta>\n");
        out.format("Page id : %d\n", pageId);
        out.format("Level : %d\n", level);
        out.format("Node Free Space : %d\n", nodeWithBufferFrame.getNodeFreeSpace());
        out.format("Buffer Free Space : %d\n", nodeWithBufferFrame.getBufferFreeSpace());
        out.format("</meta>\n");
        out.format("<children>\n");
        int i = 0;
        while(iterator.hasNext()) {
            indexTuple.reset(iterator.peek());
            out.format("%s\n",indexTuple);
            pageIds[i] = indexTuple.getPageId();
            iterator.next();
            i++;
        }
        out.format("</children>\n");
        iterator = nodeWithBufferFrame.getBufferIterator();
        out.format("<buffer>\n");
        i = 0;
        while(iterator.hasNext()) {
            out.format("Item %d : %s\n", i, iterator.peek());
            iterator.next();
            i++;
        }
        out.format("</buffer>\n");
        out.format("</node>\n\n");

        nodePage.releaseReadLatch();

        if(nodeWithBufferFrame.isLeafNode()) {
            bufferCache.unpin(nodePage);
            for(int j = 0 ; j < pageIds.length; j++) {
                printLeaf(pageIds[j], out);
            }
        } else {
            bufferCache.unpin(nodePage);
            for(int j = 0 ; j < pageIds.length; j++) {
                printNode(level - 1, pageIds[j], out);
            }
        }

    }

    public void printTree(PrintStream out) throws HyracksDataException {

        metaFrame.getPage().acquireReadLatch();
        out.format("<metadata>\n");
        out.format("Height : %d\n", metaFrame.getHeight());
        out.format("Max Page Id : %d\n", metaFrame.getMaxPage());
        out.format("</metadata>\n\n");


        //bufferManager.printActiveBuffers(out);
        IOrderedTupleIterator iterator = null;

        int level = metaFrame.getHeight() - 1;
        metaFrame.getPage().releaseReadLatch();
        ITreeNodeFrame nodeFrame = rootNodeFrameFactory.createFrame();
        ICachedPage nodePage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPageId), false);
        nodeFrame.setPage(nodePage);
        nodePage.acquireReadLatch();

        int[] pageIds = new int[nodeFrame.getChildrenCount()];
        IIndexTupleReference indexTuple = writer.createNewIndexTuple();
        iterator = nodeFrame.getChildrenIterator();
        out.format("<rootnode>\n");
        out.format("<meta>\n");
        out.format("Page id : %d\n", rootPageId);
        out.format("Level : %d\n", level);
        out.format("Node Free Space : %d\n", nodeFrame.getNodeFreeSpace());
        out.format("</meta>\n");
        out.format("<children>\n");
        int i = 0;
        while(iterator.hasNext()) {
            indexTuple.reset(iterator.peek());
            out.format("%s\n", indexTuple);
            pageIds[i] = indexTuple.getPageId();
            iterator.next();
            i++;
        }
        out.format("</children>\n");
        out.format("</rootnode>\n\n");

        nodePage.releaseReadLatch();

        if(nodeFrame.isLeafNode()) {
            bufferCache.unpin(nodePage);
            for(int j = 0 ; j < pageIds.length; j++) {
                printLeaf(pageIds[j], out);
            }
        } else {
            bufferCache.unpin(nodePage);
            for(int j = 0 ; j < pageIds.length; j++) {
                printNode(level - 1, pageIds[j], out);
            }
        }
    }

    public boolean hasBloomFilters() {
        return false;
    }

    public boolean needsDupKeyCheck() {
        return needsDupKeyCheck;
    }

    public class BufferTreeIndexAccessor implements IIndexAccessor {

        protected BufferTree tree;
        protected PointSearchCursor cursor;
        protected PointSearchPredicate pred;

        public BufferTreeIndexAccessor(BufferTree tree) {
            this.tree = tree;
            this.pred = new PointSearchPredicate(null);
        }

        public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
            if(tree.needsDupKeyCheck()) {
                cursor = (PointSearchCursor) createSearchCursor(false);
                pred.searchKey = tuple;
                tree.search(cursor, pred);
                if(cursor.hasNext()) {
                    if(comparator.compare(cursor.getTuple(), tuple) == 0) {
                        throw new IndexException("Record with same key already exists!");
                    }
                }
            }
            tree.insert(tuple);
        }

        public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
            throw new AssertionError("Operation not yet supported");
        }

        public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
            throw new AssertionError("Operation not yet supported");
        }

        public void upsert(ITupleReference tuple) throws HyracksDataException, IndexException {
            throw new AssertionError("Operation not yet supported");
        }

        public IIndexCursor createSearchCursor(boolean exclusive) {
            if(!exclusive) {
                if(cursor == null) {
                    cursor = new PointSearchCursor(tree);
                }
                return cursor;
            } else {
                return new PointSearchCursor(tree);
            }
        }

        public void search(IIndexCursor cursor, ISearchPredicate searchPredicate) throws HyracksDataException, IndexException {
            tree.search(cursor, searchPredicate);
        }
    }
}
