/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
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
import java.io.FileFilter;
import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import krati.Mode;
import krati.io.Closeable;

import org.apache.log4j.Logger;

/**
 * SegmentManager
 * 
 * <pre>
 *    SegmentManager manager = new SegmentManager(...);
 *    Segment segment = manager.nextSegment();
 *    
 *    while(...) {
 *        try {
 *            segment.append(...);
 *        } catch(SegmentOverflowException e) {
 *           segment.force();
 *           manager.updateMeta();
 *           segment = manger.nextSegment();
 *        }
 *    }
 * </pre>
 * 
 * @author jwu
 * @since 02/05, 2010
 * 
 * <p>
 * 05/24, 2010 - Always try to open the manager upon call to SegmentManager.getInstance(...) <br/>
 * 02/14, 2012 - Remove the last segment file after being freed <br/>
 */
public final class SegmentManager implements Closeable {
    private final static Logger _log = Logger.getLogger(SegmentManager.class);
    private final static Map<String, SegmentManager> _segManagerMap = new HashMap<String, SegmentManager>();
    
    /**
     * The list of segments.
     */
    private final List<Segment> _segList = new ArrayList<Segment>(100);
    
    /**
     * The list of segments that were recycled for reuse.
     */
    private final LinkedList<Segment> _recycleList = new LinkedList<Segment>();
    
    /**
     * The segment factory.
     */
    private final SegmentFactory _segFactory;
    
    /**
     * The home path where all the segment files are located.
     */
    private final String _segHomePath;
    
    /**
     * The segment file size in MB.
     */
    private final int _segFileSizeMB;
    
    /**
     * The limit on the number of recycled segments.
     */
    private final int _recycleLimit;
    
    /**
     * The meta data for all the managed segments.
     */
    private volatile SegmentMeta _segMeta = null;
    
    /**
     * The current segment.
     */
    private volatile Segment _segCurrent = null;
    
    /**
     * The mode can only be <code>Mode.INIT</code>, <code>Mode.OPEN</code> and <code>Mode.CLOSED</code>.
     */
    private volatile Mode _mode = Mode.INIT;
    
    private SegmentManager(String segmentHomePath) throws IOException {
        this(segmentHomePath, new MappedSegmentFactory());
    }

    private SegmentManager(String segmentHomePath, SegmentFactory segmentFactory) throws IOException {
        this(segmentHomePath, segmentFactory, Segment.defaultSegmentFileSizeMB);
    }

    private SegmentManager(String segmentHomePath, SegmentFactory segmentFactory, int segmentFileSizeMB) throws IOException {
        _log.info("init segHomePath=" + segmentHomePath + " segFileSizeMB=" + segmentFileSizeMB);

        this._segFactory = segmentFactory;
        this._segHomePath = segmentHomePath;
        this._segFileSizeMB = segmentFileSizeMB;
        this._recycleLimit = computeRecycleLimit(segmentFileSizeMB);
        this.open();
    }
    
    /**
     * Computes the recycle limit based on the segment file size in MB.
     * 
     * @param segmentFileSizeMB - the segment file size in MB
     * @return the recycle limit
     */
    private int computeRecycleLimit(int segmentFileSizeMB) {
        // Should always return an integer greater than zero.
        return (segmentFileSizeMB <= 64) ? 5 : ((segmentFileSizeMB <= 256) ? 3 : 2);
    }
    
    /**
     * Gets the segment file size in MB.
     */
    public int getSegmentFileSizeMB() {
        return _segFileSizeMB;
    }
    
    /**
     * Gets the file path to segment home.
     */
    public String getSegmentHomePath() {
        return _segHomePath;
    }
    
    /**
     * Gets the segment factory.
     */
    public SegmentFactory getSegmentFactory() {
        return _segFactory;
    }
    
    /**
     * Gets the current segment.
     */
    public Segment getCurrentSegment() {
        return _segCurrent;
    }
    
    /**
     * Gets the segment at the specified <code>index</code>.
     * 
     * @param index - the segment index (i.e., segmentId)
     */
    public Segment getSegment(int index) {
        return _segList.get(index);
    }
    
    /**
     * Gets the count of segments managed by this SegmentManager.
     */
    public int getSegmentCount() {
        return _segList.size();
    }
    
    /**
     * Gets the count of live segments managed by this SegmentManager.
     */
    public int getLiveSegmentCount() {
        int num = 0;

        for (int i = 0; i < _segList.size(); i++) {
            if (_segList.get(i) != null)
                num++;
        }

        return num;
    }
    
    /**
     * Clears this SegmentManger.
     */
    public synchronized void clear() {
        clearInternal(true /* CLEAR META */);
    }
    
    /**
     * Frees the specified segment.
     */
    public synchronized boolean freeSegment(Segment seg) throws IOException {
        if (seg == null)
            return false;

        int segId = seg.getSegmentId();
        if (segId < _segList.size() && _segList.get(segId) == seg) {
            _segList.set(segId, null);
            seg.close(false);
            
            if(segId == (_segList.size() - 1)) {
                try {
                    // Delete the last segment.
                    _segList.remove(segId);
                    seg.getSegmentFile().delete();
                    _log.info("Segment " + seg.getSegmentId() + " deleted");
                } catch(Exception e) {
                    _log.warn("Segment " + seg.getSegmentId() + " not deleted", e);
                }
            } else {
                if (seg.isRecyclable() && recycle(seg)) {
                    _log.info("Segment " + seg.getSegmentId() + " recycled");
                } else {
                    _log.info("Segment " + seg.getSegmentId() + " freed");
                }
            }
            
            return true;
        }
        
        return false;
    }
    
    /**
     * Gets the next segment available for read and write.
     */
    public synchronized Segment nextSegment() throws IOException {
        _segCurrent = nextSegment(false);
        return _segCurrent;
    }
    
    /**
     * Gets the next segment available for read and write.
     * 
     * @param newOnly
     *            If true, create a new segment from scratch. Otherwise, reuse
     *            the first free segment.
     * @return
     * @throws IOException
     */
    private synchronized Segment nextSegment(boolean newOnly) throws IOException {
        int index;
        Segment seg;

        if (newOnly) {
            index = _segList.size();
        } else {
            if (_recycleList.size() > 0) {
                seg = _recycleList.remove();
                seg.reinit();

                _segList.set(seg.getSegmentId(), seg);
                _log.info("reinit Segment " + seg.getSegmentId());
                return seg;
            }

            for (index = 0; index < _segList.size(); index++) {
                if (_segList.get(index) == null)
                    break;
            }
        }

        // Always create next segment as READ_WRITE
        File segFile = new File(_segHomePath, index + ".seg");
        seg = getSegmentFactory().createSegment(index, segFile, _segFileSizeMB, Segment.Mode.READ_WRITE);

        if (index < _segList.size())
            _segList.set(index, seg);
        else
            _segList.add(seg);

        return seg;
    }
    
    /**
     * Initializes the segment meta file.
     * 
     * @throws IOException
     */
    private void initMeta() throws IOException {
        _segMeta = new SegmentMeta(new File(_segHomePath, ".meta"));
    }
    
    /**
     * Initializes the segments managed by this SegmentManager
     * 
     * @throws IOException
     */
    private void initSegs() throws IOException {
        int loaded = 0;
        File[] segFiles = listSegmentFiles();
        if (segFiles.length == 0) {
            return;
        }

        try {
            for (int i = 0; i < segFiles.length; i++) {
                File segFile = segFiles[i];
                int segId = Integer.parseInt(segFile.getName().substring(0, segFile.getName().indexOf('.')));
                if (segId != i) {
                    throw new IOException("Segment file " + i + ".seg missing");
                }

                if (getMeta().hasSegmentInService(segId)) {
                    // Always load a live segment as READ_ONLY
                    Segment s = getSegmentFactory().createSegment(segId, segFile, _segFileSizeMB, Segment.Mode.READ_ONLY);
                    s.incrLoadSize(getMeta().getSegmentLoadSize(segId));
                    _segList.add(s);
                    loaded++;
                } else {
                    // Segment is not live and is free for reuse
                    _segList.add(null);
                }
            }
        } catch (IOException e) {
            _log.error(e.getMessage());
            
            clearInternal(false /* DO NOT CLEAR META */);
            throw e;
        }

        _log.info("loaded: " + loaded + "/" + segFiles.length);
    }
    
    /**
     * Clears this SegmentManager.
     * 
     * @param clearMeta - whether to clear the segment meta file
     */
    private void clearInternal(boolean clearMeta) {
        // Close all known segments
        for(int segId = 0, cnt = _segList.size(); segId < cnt; segId++) {
            Segment seg = _segList.get(segId);
            if(seg != null) {
                try {
                    seg.close(false);
                } catch (IOException e) {
                    _log.warn("failed to close segment " + seg.getSegmentId());
                } finally {
                    _segList.set(segId, null);
                }
            }
        }
        
        if(clearMeta) {
            try {
                updateMeta();
            } catch (IOException e) {
                _log.warn("failed to clear segment meta");
            }
        }
        
        _segList.clear();
        _segCurrent = null;
        _recycleList.clear();
    }
    
    /**
     * Recycle a free segment into the <code>_recycleList</code>.
     * 
     * @param seg - the free Segment
     * @return <code>true</code> if the specified segment is added to the <code>_recycleList</code>.
     */
    private boolean recycle(Segment seg) {
        if(_recycleList.size() < _recycleLimit) {
            return _recycleList.add(seg);
        }
        
        return false;
    }
    
    /**
     * Lists all the segment files managed by this SegmentManager. 
     */
    protected File[] listSegmentFiles() {
        File segDir = new File(_segHomePath);
        File[] segFiles = segDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File filePath) {
                String fileName = filePath.getName();
                if (fileName.matches("^[0-9]+\\.seg$")) {
                    return true;
                }
                return false;
            }
        });

        if (segFiles == null) {
            segFiles = new File[0];
        } else if (segFiles.length > 0) {
            Arrays.sort(segFiles, new Comparator<File>() {
                @Override
                public int compare(File f1, File f2) {
                    int segId1 = Integer.parseInt(f1.getName().substring(0, f1.getName().indexOf('.')));
                    int segId2 = Integer.parseInt(f2.getName().substring(0, f2.getName().indexOf('.')));
                    return (segId1 < segId2) ? -1 : ((segId1 == segId2) ? 0 : 1);
                }
            });
        }

        return segFiles;
    }
    
    /**
     * Gets the meta data of all the segments managed by this SegmentManager.
     */
    public SegmentMeta getMeta() {
        return _segMeta;
    }
    
    /**
     * Updates the meta data of all the segments accordingly.
     * 
     * @throws IOException
     */
    public synchronized void updateMeta() throws IOException {
        _segMeta.wrap(this);
    }
    
    /**
     * Gets the instance of SegmentManager for the specified <code>segmentHomePath</code>. 
     * 
     * @param segmentHomePath   - the file path to segment home.
     * @param segmentFactory    - the segment factory
     * @param segmentFileSizeMB - the segment file size in MB
     * @return the instance of SegmentManager
     * @throws IOException
     */
    public synchronized static SegmentManager getInstance(String segmentHomePath, SegmentFactory segmentFactory, int segmentFileSizeMB) throws IOException {
        if (segmentFileSizeMB < Segment.minSegmentFileSizeMB) {
            throw new IllegalArgumentException("Invalid argument segmentFileSizeMB " + segmentFileSizeMB + ", smaller than " + Segment.minSegmentFileSizeMB);
        }

        if (segmentFileSizeMB > Segment.maxSegmentFileSizeMB) {
            throw new IllegalArgumentException("Invalid argument segmentFileSizeMB " + segmentFileSizeMB + ", greater than " + Segment.maxSegmentFileSizeMB);
        }

        File segDir = new File(segmentHomePath);
        if (!segDir.exists()) {
            if (!segDir.mkdirs()) {
                throw new IOException("Failed to create directory " + segmentHomePath);
            }
        }

        if (segDir.isFile()) {
            throw new IOException("File " + segmentHomePath + " is not a directory");
        }

        String key = segDir.getCanonicalPath();
        SegmentManager mgr = _segManagerMap.get(key);
        if (mgr == null) {
            mgr = new SegmentManager(key, segmentFactory, segmentFileSizeMB);
            _segManagerMap.put(key, mgr);
        }

        mgr.open();
        return mgr;
    }

    @Override
    public synchronized void close() throws IOException {
        if(_mode == Mode.CLOSED) {
            return;
        }
        
        try {
            clearInternal(false /* DO NOT CLEAR META */);
            if(_segMeta != null) {
                _segMeta.close();
            }
        } catch(Exception e) {
            _log.error("Failed to close", e);
        } finally {
            _segMeta = null;
        }
        
        // The manager is closed properly now.
        _mode = Mode.CLOSED;
    }
    
    @Override
    public synchronized void open() throws IOException {
        if(_mode == Mode.OPEN) return;
        
        // Initialize segment meta data.
        initMeta();
        
        // Initialize all known segments.
        try {
            initSegs();
        } catch(Exception e) {
            this.close();
            
            // Throw original exception if possible 
            throw (e instanceof IOException) ? (IOException)e : new IOException(e);
        }
        
        // The manager is opened properly now.
        _mode = Mode.OPEN;
    }
    
    @Override
    public boolean isOpen() {
        return _mode == Mode.OPEN;
    }
}
