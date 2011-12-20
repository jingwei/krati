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

/**
 * SegmentReadOnlyException
 * 
 * @author jwu
 * 
 */
public class SegmentReadOnlyException extends SegmentException {
    private final static long serialVersionUID = 1L;
    private final Segment segment;

    public SegmentReadOnlyException(Segment seg) {
        super("Failed to write to read-only segment: " + seg.getSegmentId());
        this.segment = seg;
    }

    public Segment getSegment() {
        return segment;
    }
}
