package test.cds;

import java.io.File;
import java.io.IOException;

import krati.cds.impl.array.AddressArray;
import krati.cds.impl.array.SimpleDataArray;
import krati.cds.impl.array.basic.RecoverableLongArray;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.segment.SegmentManager;
import test.AbstractSeedTest;

/**
 * Test SimpleDataArray.
 * 
 * @author jwu
 *
 */
public class TestSimpleArray extends AbstractSeedTest
{
    public TestSimpleArray()
    {
        super(TestSimpleArray.class.getSimpleName());
    }
    
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.ChannelSegmentFactory();
    }
    
    protected AddressArray getAddressArray(File homeDir) throws Exception
    {
        return new RecoverableLongArray(idCount, 10000, 5, homeDir);
    }
    
    protected SegmentManager getSegmentManager(File homeDir) throws IOException
    {
        String segmentHome = homeDir.getCanonicalPath() + File.separator + "segs";
        return SegmentManager.getInstance(segmentHome, getSegmentFactory(), segFileSizeMB);
    }
    
    protected SimpleDataArray getDataArray(File homeDir) throws Exception
    {
        SimpleDataArray array = new SimpleDataArray(getAddressArray(homeDir),
                                                    getSegmentManager(homeDir));
        return array;
    }
    
    public void populate(SimpleDataArray dataArray) throws IOException
    {
        String line;
        int index = 0;
        int length = dataArray.length();
        int lineCnt = _lineSeedData.size();
        
        long scn = dataArray.getHWMark();
        long startTime = System.currentTimeMillis();
        
        while(index < length)
        {
            try
            {
                line = _lineSeedData.get(index % lineCnt);
                dataArray.setData(index, line.getBytes(), scn++);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            index++;
        }
        
        dataArray.persist();

        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        System.out.printf("elapsedTime=%d ms (init)%n", elapsedTime);
        
        double rate = dataArray.length()/(double)elapsedTime;
        System.out.printf("writeCount=%d rate=%6.2f per ms%n", dataArray.length(), rate);
    }
    
    public void testPopulate()
    {
        TestSimpleArray eval = new TestSimpleArray();
        
        try
        {
            AbstractSeedTest.loadSeedData();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            return;
        }
        
        try
        {
            SimpleDataArray array;
            
            File homeDir = new File(TEST_OUTPUT_DIR, getClass().getSimpleName());
            array = getDataArray(homeDir);
            
            System.out.println("---populate---");
            eval.populate(array);
            
            array.sync();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}
