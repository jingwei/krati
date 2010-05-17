package test.cds;

import java.io.File;

import krati.cds.DataCache;
import krati.cds.impl.DataCacheImpl;

/**
 * TestDataCache using MappedSegment 
 * 
 * @author jwu
 *
 */
public class TestDataCacheMapped extends TestDataCache
{
    @Override
    protected DataCache getDataCache(File cacheDir) throws Exception
    {
        DataCache cache = new DataCacheImpl(idStart,
                                            idCount,
                                            cacheDir,
                                            new krati.cds.impl.segment.MappedSegmentFactory(),
                                            segFileSizeMB);
        return cache;
    }
}
