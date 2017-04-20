package org.apache.hyracks.storage.am.buffertree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.buffertree.api.*;
import org.apache.hyracks.storage.am.buffertree.api.frames.*;
import org.apache.hyracks.storage.am.buffertree.common.BloomFilterFactory;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueComparator;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTuple;
import org.apache.hyracks.storage.am.buffertree.common.KeyValueTupleWriter;
import org.apache.hyracks.storage.am.buffertree.frames.*;
import org.apache.hyracks.storage.am.common.api.IFreePageManager;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import org.apache.hyracks.storage.common.buffercache.*;
import org.apache.hyracks.storage.common.file.TransientFileMapManager;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadFactory;

public class BufferTreeUtils{

    public static ITupleReference generateRandomKeyValue(Random gen) {
        long key = Math.abs(gen.nextLong());
        ByteBuffer buf = ByteBuffer.allocate(200);
        buf.put(KeyValueTuple.PUT);
        buf.putShort((short)8);
        buf.putShort((short)187);
        buf.putLong(key);
        KeyValueTuple tuple = new KeyValueTuple();
        tuple.reset(buf.array(), 0, 200);
        return tuple;
    }

    public static BufferTree createTree(String folderPath, String fileName, int pageSize, int nodeSize,
                                        int bufferSize, int memPages, int numBucketPages, int numBuckets,
                                        boolean needsDupKeyCheck, boolean hasBloomFilter, boolean slottedBuffer) {

        int recordSize = 204;
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy pageReplacementStrategy = new ClockPageReplacementStrategy(allocator, pageSize, memPages);
        IPageCleanerPolicy pageCleanerPolicy = new DelayPageCleanerPolicy(1000);

        ThreadFactory threadFactory = new ThreadFactory() {
            public Thread newThread(Runnable r) {
                return new Thread(r);
            }
        };
        TransientFileMapManager fileMapManager = new TransientFileMapManager();
        IODeviceHandle ioDeviceHandle = new IODeviceHandle(new File(folderPath), "");
        FileReference fileReference = ioDeviceHandle.createFileReference(fileName);
        ArrayList<IODeviceHandle> handles = new ArrayList<IODeviceHandle>();
        handles.add(ioDeviceHandle);
        IOManager ioManager = null;
        try {
            ioManager = new IOManager(handles);
        } catch (HyracksException e) {
            e.printStackTrace();
        }
        IBufferCache bufferCache = new BufferCache(ioManager, pageReplacementStrategy, pageCleanerPolicy, fileMapManager, 5, threadFactory);

        ITupleWriter writer = KeyValueTupleWriter.INSTANCE;
        ITupleComparator comparator = KeyValueComparator.INSTANCE;

        ITreeMetaFrameFactory metaFrameFactory = new DefaultBufferTreeMetaFrame.Factory();
        ITreeNodeFrameFactory rootNodeFrameFactory = new BufferTreeNodeFrame.Factory(nodeSize, writer, comparator);
        ITreeNodeWithBufferFrameFactory nodeFrameFactory =null;

        if(slottedBuffer) {
            nodeFrameFactory = new BufferTreeNodeWithSlottedBufferFactory(nodeSize, bufferSize, writer, writer, comparator);
        } else {
            nodeFrameFactory = new BufferTreeNodeWithAppendOnlyBufferFactory(nodeSize, bufferSize, writer, writer, comparator);
        }

        IPrimaryBufferFrameFactory bufferFrameFactory = new SlottedPrimaryBufferFrame.Factory(pageSize, writer, comparator);
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, 0, metaFrameFactory);

        IPrimaryBufferBucketFactory bucketFactory = new PrimaryBufferBucket.Factory(numBucketPages, pageSize, bufferFrameFactory);
        ConcurrentBufferManager bufferManager = new ConcurrentBufferManager(numBuckets, numBucketPages, pageSize, bucketFactory);

        ITreeLeafFrameFactory leafFrameFactory = null;
        BufferTree tree = null;
        if(hasBloomFilter) {
            leafFrameFactory = new BufferTreeLeafFrameWithBloomFilter.Factory(pageSize, writer, comparator, BloomFilterFactory.INSTANCE);
            tree = new BufferTreeWithBloomFilters(bufferCache, fileMapManager, freePageManager, fileReference,
                    rootNodeFrameFactory, nodeFrameFactory, leafFrameFactory, bufferFrameFactory, metaFrameFactory,
                    writer, comparator, bufferManager, BloomFilterFactory.INSTANCE, needsDupKeyCheck, recordSize);
        } else {
            leafFrameFactory = new BufferTreeLeafFrame.Factory(pageSize, writer, comparator);
            tree = new BufferTree(bufferCache, fileMapManager, freePageManager, fileReference,
                    rootNodeFrameFactory, nodeFrameFactory, leafFrameFactory, bufferFrameFactory, metaFrameFactory,
                    writer, comparator, bufferManager, needsDupKeyCheck, recordSize);
        }

        return tree;
    }


    public static void main(String[] args) {
        int pageSize, nodeSize, bufferSize, memPages, numBucketPages, numBuckets;
        String folderPath, fileName, writePerformanceFile, readPerformanceFile;
        boolean needsDupKeyCheck = false, withBloomFilters = false;
        if(args.length > 1)  {
            pageSize = Integer.valueOf(args[0]) * 1024;
            nodeSize = Integer.valueOf(args[1]) * 1024;
            bufferSize = Integer.valueOf(args[2]) * 1024;
            memPages = Integer.valueOf(args[3]);
            numBucketPages = Integer.valueOf(args[4]);
            numBuckets = Integer.valueOf(args[5]);
            needsDupKeyCheck = args[6].equals("primary");
            withBloomFilters = args[7].equals("withBloomFilters");
        } else {
            pageSize = 16 * 1024;
            nodeSize = 4 * 1024;
            bufferSize = 12 * 1024;
            memPages = 1024;
            numBucketPages = 16;
            numBuckets = 8;
            needsDupKeyCheck = true;
            withBloomFilters = true;
        }

        writePerformanceFile = new String("writeStats.csv");
        readPerformanceFile = new String("readStats.csv");

        folderPath = "C:/Users/t-guje/Desktop/";
        fileName = "BufferTreeData.dat";

        PrintStream outStream = null;
        try {
            outStream  = new PrintStream(new FileOutputStream(folderPath + "BufferTreePrint.dat"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        BufferTree tree = createTree(folderPath, fileName, pageSize, nodeSize, bufferSize, memPages, numBucketPages, numBuckets, needsDupKeyCheck, withBloomFilters, true);
        PrintStream writeStats = null;
        PrintStream readStats = null;

        try {
            writeStats = new PrintStream(new FileOutputStream(folderPath + writePerformanceFile));
            //readStats = new PrintStream(new FileOutputStream(folderPath + readPerformanceFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            tree.create();
            tree.activate();

            IIndexAccessor accessor = null;
            try {
                accessor = tree.createAccessor(null, null);
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }

            int seed = 5123;
            Random gen = new Random(seed);
            int percent = 0;
            long start = System.currentTimeMillis();
            double total = 5e7;
            double log = total/1000;
            ITupleReference tuple = null;

            for(double i = 1; i <= total; i++) {
                tuple = generateRandomKeyValue(gen);
                try {
                    accessor.insert(tuple);
                } catch (IndexException e) {
                    System.out.println(e);
                }

                if((int)i % (int)log == 0) {
                    writeStats.format("%f, %d\n", (i * 200) / 1e6, (System.currentTimeMillis() - start));
                    percent++;
                    System.out.println(((double)percent)/10 + "% done");
                }
            }
            System.out.println("Tree height : " + tree.metaFrame.getHeight());

            //tree.printTree(outStream);

//            PointSearchCursor cursor = (PointSearchCursor) accessor.createSearchCursor(true);
//            PointSearchPredicate pred = new PointSearchPredicate(null);
//            gen = new Random(seed);
//            start = System.currentTimeMillis();
//            percent = 0;
//            for(double i = 1; i <= total; i++) {
//                tuple = generateRandomKeyValue(gen);
//                pred.searchKey = tuple;
//                try {
//                    accessor.search(cursor, pred);
//                    if(!cursor.hasNext()) {
//                        System.out.println(pred.searchKey);
//                        throw new AssertionError();
//                    }
//
//                    if(KeyValueComparator.INSTANCE.compare(cursor.getTuple(), pred.searchKey) != 0) {
//                        throw new AssertionError();
//                    }
//
//                    if((int)i % (int)log == 0) {
//                        readStats.format("%f, %d\n", (i * 200) / 1e6, (System.currentTimeMillis() - start));
//                        percent++;
//                        System.out.println(((double)percent)/10 + "% checking done");
//                    }
//                } catch (IndexException e) {
//                    e.printStackTrace();
//                }
//            }
//            System.out.println("Tree height : " + tree.metaFrame.getHeight());
            tree.deactivate();
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }

        System.out.println("done");
    }

}
