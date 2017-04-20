package org.apache.hyracks.storage.am.buffertree.api.frames;

import com.google.common.hash.BloomFilter;


public interface ITreeLeafFrameWithBloomFilter extends ITreeLeafFrame {

    BloomFilter getBloomFilter();

}
