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

/**
 * SegmentModeListener.
 * 
 * @author jwu
 * @since 09/05, 2012
 */
public interface SegmentModeListener {
    
    /**
     * Fires a {@link SegmentModeEvent} right before a segment changes its mode.
     * 
     * @param event - the segment mode event
     */
    public void modeChange(SegmentModeEvent event);
    
    /**
     * Fires a {@link SegmentModeEvent} right after a segment changed its mode.
     *  
     * @param event - the segment mode event
     */
    public void modeChanged(SegmentModeEvent event);
    
}
