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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * SegmentIndexBufferManager
 * 
 * @author jwu
 * @since 08/27, 2012
 */
public class SegmentIndexBufferManager {
    /**
     * The queue to which segment index buffers are submitted for being flushed.
     */
    protected final ConcurrentLinkedQueue<SegmentIndexBuffer> _sibSubmit;
    
    /**
     * The map from which segment index buffers for writer/compactor are retrieved.
     */
    protected final ConcurrentHashMap<Integer, SegmentIndexBuffer> _sibLookup;
    
    /**
     * The segment index buffer IO utility. 
     */
    private volatile SegmentIndexBufferIO _sibIO;
    
    /**
     * Creates a new SegmentIndexBufferManager. 
     */
    public SegmentIndexBufferManager() {
        _sibLookup = new ConcurrentHashMap<Integer, SegmentIndexBuffer>();
        _sibSubmit = new ConcurrentLinkedQueue<SegmentIndexBuffer>();
        setSegmentIndexBufferIO(new SegmentIndexBufferFileIO());
    }
    
    /**
     * Gets the segment index buffer for the specified <code>segId</code>.
     * 
     * @param segId - the segment Id
     * @return the segment index buffer known to this SegmentIndexBufferManager.
     *         Or <code>null</code> if no segment index buffer was previously opened
     *         by the method {@link #openSegmentIndexBuffer(int)}.
     */
    public SegmentIndexBuffer getSegmentIndexBuffer(int segId) {
        return _sibLookup.get(segId);
    }
    
    /**
     * Opens a segment index buffer for the specified <code>segId</code>.
     * 
     * @param segId - the segment Id
     * @return the segment index buffer which is not <code>null</code>.
     */
    public SegmentIndexBuffer openSegmentIndexBuffer(int segId) {
        SegmentIndexBuffer sib = _sibLookup.get(segId);
        if(sib == null) {
            sib = new SegmentIndexBuffer();
            sib.setSegmentId(segId);
            _sibLookup.put(segId, sib);
        }
        return sib;
    }
    
    /**
     * Removes the specified segment index buffer if it is not <code>null</code>. 
     * 
     * @param sib - the segment index buffer
     * @return <code>true</code> if the operation is successful. Otherwise, <code>false</code>.
     */
    public boolean remove(SegmentIndexBuffer sib) {
        if(sib == null) {
            return false;
        }
        
        _sibLookup.remove(sib.getSegmentId());
        return true;
    }
    
    /**
     * Submits (i.e., offers) the specified segment index buffer if it is not <code>null</code>.
     * 
     * @param sib - the segment index buffer
     * @return <code>true</code> if the submission is successful. Otherwise, <code>false</code>.
     */
    public boolean submit(SegmentIndexBuffer sib) {
        if(sib == null) {
            return false;
        }
        
        _sibLookup.remove(sib.getSegmentId());
        return _sibSubmit.offer(sib);
    }
    
    /**
     * Polls a segment index buffer which was previously queued by calling the method {@link #submit(SegmentIndexBuffer)}.
     * 
     * @return a segment index buffer or <code>null</code>.
     */
    public SegmentIndexBuffer poll() {
        return _sibSubmit.poll();
    }
    
    /**
     * Clears all segment index buffers known to this SegmentIndexBufferManager.
     */
    public void clear() {
        _sibLookup.clear();
        _sibSubmit.clear();
    }
    
    /**
     * Gets the segment index buffer IO utility.
     */
    public SegmentIndexBufferIO getSegmentIndexBufferIO() {
        return _sibIO;
    }
    
    /**
     * Sets the segment index buffer IO utility.
     * 
     * @param sibIO - the segment index buffer IO
     * @throws NullPointerException if the specified argument is <code>null</code>. 
     */
    public void setSegmentIndexBufferIO(SegmentIndexBufferIO sibIO) {
        if(sibIO == null) {
            throw new NullPointerException();
        }
        this._sibIO = sibIO;
    }
}
