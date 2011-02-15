package test.core;

import java.io.File;

import krati.core.segment.SegmentFactory;
import krati.store.AbstractDataArray;
import krati.store.DynamicDataArray;

public class TestDynamicDataArray extends EvalDataArray {

    @Override
    protected SegmentFactory createSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }

    @Override
    protected AbstractDataArray createDataArray(File homeDir) throws Exception {
        int initialLength;
        initialLength = Math.max(_idCount, 1 << 16);
        
        DynamicDataArray dynArray =
            new DynamicDataArray(initialLength,
                                 homeDir,
                                 createSegmentFactory(),
                                 _segFileSizeMB);
        
        dynArray.set(initialLength, null, System.currentTimeMillis());
        
        dynArray.set(initialLength * 2, null, System.currentTimeMillis());
        
        dynArray.set(initialLength * 3, null, System.currentTimeMillis());
        
        return dynArray;
    }
}
