package org.apache.hyracks.storage.am.buffertree.common;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hyracks.storage.am.buffertree.api.IBloomFilterFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BloomFilterFactory implements IBloomFilterFactory {

    public static final IBloomFilterFactory INSTANCE = new BloomFilterFactory(150, 0.01);

    int expectedInsertions;
    int size;
    double fpp;

    public BloomFilterFactory(int expectedInsertions, double fpp) {
        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
        BloomFilter bf = BloomFilter.create(Funnels.byteArrayFunnel(), expectedInsertions, fpp);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            bf.writeTo(out);
            this.size = out.size();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public BloomFilter createEmptyBloomFilter() {
        return BloomFilter.create(Funnels.byteArrayFunnel(), expectedInsertions, fpp);
    }

    public int getSize() {
        return size;
    }
}
