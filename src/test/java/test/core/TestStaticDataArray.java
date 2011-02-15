package test.core;

import java.io.File;

import krati.core.segment.SegmentFactory;
import krati.store.AbstractDataArray;
import krati.store.StaticDataArray;

public class TestStaticDataArray extends EvalDataArray {
    
    @Override
    protected SegmentFactory createSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    @Override
    protected AbstractDataArray createDataArray(File homeDir) throws Exception {
        return new StaticDataArray(_idCount,
                                   homeDir,
                                   createSegmentFactory(),
                                   _segFileSizeMB);
    }
}
