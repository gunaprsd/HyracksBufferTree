package org.apache.hyracks.storage.am.buffertree.api.frames;

public interface ITreeNodeWithBufferFrameFactory {

    ITreeNodeWithBufferFrame createFrame(int level);

    int getNodeCapacity(int level);

    int getBufferCapacity(int level);

}
