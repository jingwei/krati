package test.set.api;

import java.io.File;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.Segment;
import krati.store.DataSet;
import krati.store.DynamicDataSet;

/**
 * TestDynamicDataSetApi
 * 
 * @author jwu
 * 06/06, 2011
 * 
 */
public class TestDynamicDataSetApi extends AbstractTestDataSetApi {

    @Override
    protected DataSet<byte[]> createStore(File homeDir) throws Exception {
        return new DynamicDataSet(
                homeDir,
                1,     /* initLevel */
                100,   /* batchSize */
                5,     /* numSyncBatches */
                Segment.defaultSegmentFileSizeMB,
                new MappedSegmentFactory());
    }
}
