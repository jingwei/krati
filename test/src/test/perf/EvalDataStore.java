package test.perf;

import java.io.File;
import java.io.IOException;

import krati.cds.impl.array.AddressArray;
import krati.cds.impl.array.basic.RecoverableLongArray;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.segment.SegmentManager;
import test.cds.TestSimpleStore;

public class EvalDataStore extends TestSimpleStore
{
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MemorySegmentFactory();
    }
    
    @Override
    protected AddressArray getAddressArray(File storeDir) throws Exception
    {
        return new RecoverableLongArray(5000000, 10000, 5, storeDir);
    }
    
    @Override
    protected SegmentManager getSegmentManager(File storeDir) throws IOException
    {
        String segmentHome = storeDir.getCanonicalPath() + File.separator + "segs";
        return SegmentManager.getInstance(segmentHome, getSegmentFactory(), 256);
    }
    
    public static void main(String[] args)
    {
        new EvalDataStore().run(4, 1);
        System.out.println("done");
    }
}
