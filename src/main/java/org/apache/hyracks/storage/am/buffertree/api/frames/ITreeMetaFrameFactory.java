package org.apache.hyracks.storage.am.buffertree.api.frames;

import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;

public interface ITreeMetaFrameFactory extends ITreeIndexMetaDataFrameFactory{

    ITreeMetaFrame createFrame();
}
