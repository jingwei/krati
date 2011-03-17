package test.hash;

import java.io.File;
import java.io.IOException;

import test.AbstractSeedTest;
import test.StatsLog;
import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataSet;
import krati.store.StaticDataSet;
import krati.util.HashFunction;

public abstract class EvalTieredHashFunction extends AbstractSeedTest {
    protected HashCollisionStats _collisionStats = new HashCollisionStats();
    
    protected EvalTieredHashFunction(String name) {
        super(name);
    }
    
    protected abstract HashFunction<byte[]> createHashFunction();
    
    protected SegmentFactory createSegmentFactory() {
        return new MemorySegmentFactory();
    }
    
    protected StaticDataSet createDataSet(File storeDir, int capacity, SegmentFactory segmentFactory, HashFunction<byte[]> hashFunction) throws Exception {
        return new StaticDataSet(storeDir, capacity, 10000, 5, 32, segmentFactory, hashFunction);
    }
    
    private void populate(StaticDataSet[] tieredStores) throws IOException {
        int storeCnt = tieredStores.length;
        int lineCnt = _lineSeedData.size();
        long startTime = System.currentTimeMillis();
        long[] tieredDist = new long[tieredStores.length];
        
        try {
            for (int i = 0; i < _keyCount; i++) {
                String s = _lineSeedData.get(i % lineCnt);
                String k = s.substring(0, 30) + i;
                byte[] b = k.getBytes();

                int j = 0;
                for (; j < storeCnt; j++) {
                    if (tieredStores[j].countCollisions(b) == 0) {
                        tieredStores[j].add(b);
                        tieredDist[j] += 1;
                        break;
                    }
                }

                if (j == storeCnt) {
                    j--;
                    tieredStores[j].add(b);
                    tieredDist[j] += 1;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        
        double rate = _keyCount/(double)elapsedTime;
        rate = Math.round(rate * 100) / 100.0;
        StatsLog.logger.info("writeCount="+ _keyCount +" rate="+ rate +" per ms");

        StatsLog.logger.info("Tier Distribution");
        for (int j = 0; j < tieredDist.length; j++) {
            StatsLog.logger.info("Tier" + j + " " + tieredDist[j]);
        }
        
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
    }
    
    private void collect(StaticDataSet[] tieredStores) {
        int storeCnt = tieredStores.length;
        int lineCnt = _lineSeedData.size();
        long startTime = System.currentTimeMillis();
        long[] tieredDist = new long[tieredStores.length];
        
        try {
            for (int i = 0; i < _keyCount; i++) {
                String s = _lineSeedData.get(i%lineCnt);
                String k = s.substring(0, 30) + i;
                byte[] b = k.getBytes();
                
                for (int j = 0; j < storeCnt; j++) {
                    int collisionCnt = tieredStores[j].countCollisions(b);
                    if (collisionCnt > 0) {
                        tieredDist[j] += 1;
                        _collisionStats.addCollisionCount(Math.abs(collisionCnt));
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        StatsLog.logger.info("Tier Distribution");
        for (int j = 0; j < tieredDist.length; j++) {
            StatsLog.logger.info("Tier" + j + " " + tieredDist[j]);
        }
        
        StatsLog.logger.info("Collision Distribution");
        _collisionStats.print(StatsLog.logger);
        
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        StatsLog.logger.info("elapsedTime="+ elapsedTime +" ms");
    }
    
    protected void evaluate(int... tierCapacities) throws Exception {
        try {
            AbstractSeedTest.loadSeedData();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        
        SegmentFactory segmentFactory = createSegmentFactory();
        HashFunction<byte[]> hashFunction = createHashFunction();
        
        File storeHomeDir = getHomeDirectory();
        
        StaticDataSet[] tieredStores = new StaticDataSet[tierCapacities.length];
        for (int i = 0; i < tierCapacities.length; i++) {
            tieredStores[i] = createDataSet(new File(storeHomeDir, "T" + i), tierCapacities[i], segmentFactory, hashFunction);
        }
        
        // initial populate
        StatsLog.logger.info(">>> populate");
        populate(tieredStores);
        
        // sync tiered stores
        for (DataSet<byte[]> store : tieredStores) {
            store.sync();
        }
        
        // collect collision statistics
        StatsLog.logger.info(">>> collect collision statistics");
        collect(tieredStores);
    }
    
    public void test() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        cleanTestOutput();
        
        double tier1 = 0.8;
        double tier2 = 0.4;
        double tier3 = 0.2;
        
        StatsLog.logger.info("keyCount=" + _keyCount + " tiers=" + tier1 + ":" + tier2 + ":" + tier3);
        
        int capacity1 = (int)(_keyCount * tier1);
        int capacity2 = (int)(_keyCount * tier2);
        int capacity3 = (int)(_keyCount * tier3);
        
        evaluate(capacity1, capacity2, capacity3);
        
        StatsLog.endUnit(unitTestName);
    }
}
