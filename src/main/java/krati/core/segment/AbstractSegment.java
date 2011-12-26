/*
 * Copyright (c) 2010-2011 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package krati.core.segment;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Date;

/**
 * AbstractSegment
 * 
 * @author jwu
 * 
 */
public abstract class AbstractSegment implements Segment {
    protected final int _segId;
    protected final File _segFile;
    protected final int _initSizeMB;
    protected final long _initSizeBytes;
    protected volatile long _lastForcedTime;
    protected volatile Segment.Mode _segMode;
    protected RandomAccessFile _raf = null;
    protected FileChannel _channel = null;
    protected long _storageVersion;
    
    /**
     * Methods {@link #decrLoadSize} and {@link #incrLoadSize} are not
     * synchronized as they modify two different fields.
     */
    private volatile int _incrLoadSize = 0;
    private volatile int _decrLoadSize = 0;
    
    protected AbstractSegment(int segmentId, File segmentFile, int initialSizeMB, Segment.Mode mode) throws IOException {
        this._segId = segmentId;
        this._segFile = segmentFile;
        this._initSizeMB = initialSizeMB;
        this._initSizeBytes = initialSizeMB * 1024L * 1024L;
        this._segMode = mode;
        this.init();
    }
    
    protected abstract void init() throws IOException;
    
    protected void initHeader() throws IOException {
        _incrLoadSize = 0;
        _decrLoadSize = 0;
        
        // update the time stamp of segment
        _lastForcedTime = System.currentTimeMillis();
        _storageVersion = Segment.STORAGE_VERSION;
        
        setAppendPosition(0);
        appendLong(getLastForcedTime());
        decrLoadSize(8);
        appendLong(getStorageVersion());
        decrLoadSize(8);
        force();
        
        _channel.position(Segment.dataStartPosition);
        setAppendPosition(Segment.dataStartPosition);
    }
    
    protected void loadHeader() throws IOException {
        _lastForcedTime = readLong(posLastForcedTime);
        _storageVersion = readLong(posStorageVersion);
    }
    
    protected String getHeader() {
        StringBuilder b = new StringBuilder();
        
        b.append("lastForcedTime");
        b.append('=');
        b.append(new Date(getLastForcedTime()));
        
        b.append(' ');
        
        b.append("storageVersion");
        b.append('=');
        b.append(getStorageVersion());
        
        return b.toString();
    }
    
    protected long getChannelPosition() throws IOException {
        return _channel == null ? -1 : _channel.position();
    }
    
    @Override
    public String getStatus() {
        StringBuilder b = new StringBuilder();
        
        b.append("loadSize");
        b.append('=');
        b.append(getLoadSize());
        
        b.append(' ');
        
        b.append("appendPosition");
        b.append('=');
        try {
            b.append(getAppendPosition());
        } catch (IOException ioe) {
            b.append('?');
        }
        
        b.append(' ');
        
        b.append("channelPosition");
        b.append('=');
        try {
            b.append(getChannelPosition());
        } catch (Exception e) {
            b.append('?');
        }
        
        b.append(' ');
        
        b.append("lastForcedTime");
        b.append('=');
        b.append(new Date(getLastForcedTime()));
        
        return b.toString();
    }
    
    @Override
    public final Mode getMode() {
        return _segMode;
    }
    
    @Override
    public final int getSegmentId() {
        return _segId;
    }
    
    @Override
    public final File getSegmentFile() {
        return _segFile;
    }
    
    @Override
    public final int getInitialSizeMB() {
        return _initSizeMB;
    }
    
    @Override
    public final long getInitialSize() {
        return _initSizeBytes;
    }
    
    @Override
    public final long getLastForcedTime() {
        return _lastForcedTime;
    }
    
    @Override
    public final long getStorageVersion() {
        return _storageVersion;
    }
    
    @Override
    public final double getLoadFactor() {
        return ((double) getLoadSize()) / _initSizeBytes;
    }
    
    @Override
    public final int getLoadSize() {
        return (_incrLoadSize - _decrLoadSize);
    }
    
    /**
     * Increases the load size.
     * 
     * This method is not synchronized. This is because that the writer and
     * compactor never write (i.e. append) data to the same segment.
     * Consequently this method is never concurrently called by the writer and
     * compactor on the same segment.
     */
    @Override
    public final void incrLoadSize(int byteCnt) {
        _incrLoadSize += byteCnt;
    }
    
    /**
     * Decreases the load size.
     * 
     * This method is not synchronized. This because that the writer is the only
     * thread calling this method on Segment(s) and the compact should never
     * call this method.
     */
    @Override
    public final void decrLoadSize(int byteCnt) {
        _decrLoadSize += byteCnt;
    }
    
    @Override
    public final boolean isReadOnly() {
        return (_segMode == Segment.Mode.READ_ONLY);
    }

    protected int getRafSizeInMB() throws IOException {
        return (int) (_raf.length() / 1024L / 1024L);
    }

    protected void checkSegmentSize() throws IOException {
        if (_raf.length() != getInitialSize()) {
            throw new SegmentFileSizeException(getSegmentFile().getCanonicalPath(), getRafSizeInMB(), getInitialSizeMB());
        }
    }
}
