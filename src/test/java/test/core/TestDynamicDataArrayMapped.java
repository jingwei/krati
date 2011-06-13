package test.core;

import java.io.File;

import krati.core.array.basic.DynamicConstants;
import krati.core.segment.SegmentFactory;
import krati.store.AbstractDataArray;
import krati.store.DynamicDataArray;

/**
 * TestDynamicDataArrayMapped
 * 
 * @author jwu
 * 
 */
public class TestDynamicDataArrayMapped extends EvalDataArray {

    @Override
    protected SegmentFactory createSegmentFactory() {
        return new krati.core.segment.MappedSegmentFactory();
    }

    @Override
    protected AbstractDataArray createDataArray(File homeDir) throws Exception {
        int initialLength;
        initialLength = Math.max(_idCount, 1 << DynamicConstants.SUB_ARRAY_BITS);
        
        DynamicDataArray dynArray =
            new DynamicDataArray(initialLength,
                                 homeDir,
                                 createSegmentFactory(),
                                 _segFileSizeMB);
        
        dynArray.set(initialLength, null, System.currentTimeMillis());
        
        if(initialLength < (1 << 20)) {
            dynArray.set(initialLength * 2, null, System.currentTimeMillis());
        }
        
        return dynArray;
    }
}
