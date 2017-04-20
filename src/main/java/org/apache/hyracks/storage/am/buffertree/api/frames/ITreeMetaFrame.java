package org.apache.hyracks.storage.am.buffertree.api.frames;

import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;

public interface ITreeMetaFrame extends ITreeIndexMetaDataFrame, ICachedPageFrame {

    byte META_PAGE_LEVEL = -1;

    int getHeight();

    void setHeight(int height);

    void incrementHeight();

    int getNextSerialForBuffer();

    int getLastSerialIdForBuffer();

    void setLastEmptiedBufferSerialId(int serialId);

    int getLastEmptiedBufferSerialId();
}
