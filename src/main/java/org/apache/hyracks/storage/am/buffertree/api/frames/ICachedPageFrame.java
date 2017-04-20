package org.apache.hyracks.storage.am.buffertree.api.frames;

import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import java.nio.ByteBuffer;

public interface ICachedPageFrame {

    ICachedPage getPage();

    void setPage(ICachedPage page);

    ByteBuffer getBuffer();
}
