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

package krati.examples;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import krati.core.StoreFactory;
import krati.core.StorePartitionConfig;
import krati.core.segment.MemorySegmentFactory;
import krati.io.Closeable;
import krati.store.ArrayStorePartition;

/**
 * Sample code for Krati Partition.
 * 
 * @author jwu
 * 
 */
public class KratiDataPartition implements Closeable {
    private final ArrayStorePartition _partition;
    
    /**
     * Constructs KratiDataPartition.
     * 
     * @param homeDir    the home directory.
     * @param idStart    the start of member IDs.
     * @param idCount    the count of member IDs.
     * @throws Exception if a partition instance can not be created.
     */
    public KratiDataPartition(File homeDir, int idStart, int idCount) throws Exception {
        StorePartitionConfig config = new StorePartitionConfig(homeDir, idStart, idCount);
        config.setSegmentFactory(new MemorySegmentFactory());
        config.setSegmentFileSizeMB(64);
        
        _partition = StoreFactory.createArrayStorePartition(config);
    }
    
    /**
     * @return the underlying data partition.
     */
    public final ArrayStorePartition getPartition() {
        return _partition;
    }
    
    /**
     * Creates data for a given member.
     * Subclasses can override this method to provide specific data for a given member.
     */
    protected byte[] createDataForMember(int memberId) {
        return ("Here is your data for member " + memberId).getBytes();
    }
    
    /**
     * Populates the underlying data partition.
     * 
     * @throws Exception
     */
    public void populate() throws Exception {
        for (int i = 0, cnt = _partition.getIdCount(); i < cnt; i++) {
            int memberId = _partition.getIdStart() + i;
            _partition.set(memberId, createDataForMember(memberId), System.nanoTime());
        }
        _partition.sync();
    }
    
    /**
     * Perform a number of random reads from the underlying data partition.
     * 
     * @param readCnt the number of reads
     */
    public void doRandomReads(int readCnt) {
        Random rand = new Random();
        int idStart = _partition.getIdStart();
        int idCount = _partition.getIdCount();
        for (int i = 0; i < readCnt; i++) {
            int memberId = idStart + rand.nextInt(idCount);
            System.out.printf("MemberId=%-10d MemberData=%s%n", memberId, new String(_partition.get(memberId)));
        }
    }
    
    /**
     * Checks if this <code>KratiDataPartition</code> is open for operations.
     */
    @Override
    public boolean isOpen() {
        return _partition.isOpen();
    }
    
    /**
     * Opens this <code>KratiDataPartition</code>.
     * 
     * @throws IOException
     */
    @Override
    public void open() throws IOException {
        _partition.open();
    }
    
    /**
     * Closes this <code>KratiDataPartition</code>.
     * 
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        _partition.close();
    }
    
    /**
     * java -Xmx4G krati.examples.KratiDataPartition homeDir idStart idCount
     */
    public static void main(String[] args) {
        try {
            // Parse arguments: homeDir idStart idCount
            File homeDir = new File(args[0]);
            int idStart = Integer.parseInt(args[1]);
            int idCount = Integer.parseInt(args[2]);
            
            // Create an instance of KratiDataPartition
            KratiDataPartition p = new KratiDataPartition(homeDir, idStart, idCount);
            
            // Populate data partition
            p.populate();
            
            // Perform some random reads from data partition.
            p.doRandomReads(10);
            
            // Close data partition
            p.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
