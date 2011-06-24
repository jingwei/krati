package test.core.api;

import java.io.File;

import krati.core.array.AddressArray;
import krati.core.array.basic.DynamicLongArray;

/**
 * TestDynamicLongArray
 * 
 * @author jwu
 * 06/21, 2011
 * 
 */
public class TestDynamicLongArray extends AbstractTestDynamicAddressArray {

    @Override
    protected AddressArray createAddressArray(File homeDir) throws Exception {
        return new DynamicLongArray(getBatchSize(), getNumSyncBatches(), homeDir);
    }
}
