package test.set.api;

import java.io.File;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.Segment;
import krati.store.DataSet;
import krati.store.StaticDataSet;

/**
 * TestStaticDataSetApi
 * 
 * @author jwu
 * 06/06, 2011
 * 
 */
public class TestStaticDataSetApi extends AbstractTestDataSetApi {

    @Override
    protected DataSet<byte[]> createStore(File homeDir) throws Exception {
        return new StaticDataSet(
                homeDir,
                10000, /* capacity */
                100,   /* batchSize */
                5,     /* numSyncBatches */
                Segment.minSegmentFileSizeMB,
                new MappedSegmentFactory());
    }
}
