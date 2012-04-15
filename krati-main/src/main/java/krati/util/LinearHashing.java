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

package krati.util;

/**
 * LinearHashing
 * 
 * @author jwu
 * 06/07, 2011
 * 
 */
public final class LinearHashing {
    private int _split;
    private int _level;
    private int _levelCapacity;
    private final int _unitCapacity;
    
    public LinearHashing(int unitCapacity) {
        if(unitCapacity < 1) {
            throw new IllegalArgumentException("Invalid argument " + unitCapacity);
        }
        
        this._unitCapacity = unitCapacity;
        this.reinit(unitCapacity);
    }
    
    public void reinit(int capacity) {
        if(capacity < 0) {
            throw new IllegalArgumentException("Invalid argument " + capacity);
        }
        
        int unitCount = capacity / _unitCapacity;
        
        if(unitCount <= 1) {
            _level = 0;
            _split = 0;
            _levelCapacity = _unitCapacity;
        } else {
            // Determine level and split
            _level = 0;
            int remainder = (unitCount - 1) >> 1;
            while(remainder > 0) {
                _level++;
                remainder = remainder >> 1;
            }
            
            _split = (unitCount - (1 << _level) - 1) * _unitCapacity;
            _levelCapacity = _unitCapacity << _level;
        }
    }
    
    public int getSplit() {
        return _split;
    }
    
    public int getLevel() {
        return _level;
    }
    
    public int getLevelCapacity() {
        return _levelCapacity;
    }
    
    public int getUnitCapacity() {
        return _unitCapacity;
    }
}
