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
 * SegmentOverflowException
 * 
 * @author jwu
 * 
 */
public class SegmentOverflowException extends SegmentException {
    private final static long serialVersionUID = 1L;
    private final Segment _segment;
    private final Type _overflowType;

    public SegmentOverflowException(Segment seg) {
        super("Overflow at segment: " + seg.getSegmentId());
        this._segment = seg;
        this._overflowType = Type.WRITE_OVERFLOW;
    }

    public SegmentOverflowException(Segment seg, Type type) {
        super(type + " at segment: " + seg.getSegmentId());
        this._segment = seg;
        this._overflowType = type;
    }

    public Segment getSegment() {
        return _segment;
    }

    public Type getOverflowType() {
        return _overflowType;
    }

    public static enum Type {
        READ_OVERFLOW {
            public String toString() {
                return "Read overflow";
            }
        },
        WRITE_OVERFLOW {
            public String toString() {
                return "Write overflow";
            }
        };
    }
}
