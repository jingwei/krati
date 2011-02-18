package test;

import java.io.PrintStream;
import java.util.Arrays;

import org.apache.log4j.Logger;

/**
 * LatencyStats
 * 
 * @author jwu
 * 
 */
public class LatencyStats {
    final long[] _latencyCountArray = new long[15];
    
    public LatencyStats() {
        Arrays.fill(_latencyCountArray, 0);
    }
    
    public void countLatency(int microSeconds) {
        int ind = 0;
        
        if(microSeconds <= 1) {
            ind = 0;
        } else if(microSeconds <= 5) {
            ind = 1;
        } else if(microSeconds <= 10) {
            ind = 2;
        } else if(microSeconds <= 50) {
            ind = 3;
        } else if(microSeconds <= 100) {
            ind = 4;
        } else {
            int i = microSeconds/100;
            ind = 4 + ((i < 10) ? i : 10);
        }
        
        _latencyCountArray[ind] += 1; 
    }
    
    public void print(PrintStream out) {
        out.println("   1," + _latencyCountArray[0]);
        out.println("   5," + _latencyCountArray[1]);
        out.println("  10," + _latencyCountArray[2]);
        out.println("  50," + _latencyCountArray[3]);
        out.println(" 100," + _latencyCountArray[4]);
        out.println(" 200," + _latencyCountArray[5]);
        out.println(" 300," + _latencyCountArray[6]);
        out.println(" 400," + _latencyCountArray[7]);
        out.println(" 500," + _latencyCountArray[8]);
        out.println(" 600," + _latencyCountArray[9]);
        out.println(" 700," + _latencyCountArray[10]);
        out.println(" 800," + _latencyCountArray[11]);
        out.println(" 900," + _latencyCountArray[12]);
        out.println("1000," + _latencyCountArray[13]);
        out.println("1+ms," + _latencyCountArray[14]);
    }
    
    public void print(Logger log) {
        log.info("   1," + _latencyCountArray[0]);
        log.info("   5," + _latencyCountArray[1]);
        log.info("  10," + _latencyCountArray[2]);
        log.info("  50," + _latencyCountArray[3]);
        log.info(" 100," + _latencyCountArray[4]);
        log.info(" 200," + _latencyCountArray[5]);
        log.info(" 300," + _latencyCountArray[6]);
        log.info(" 400," + _latencyCountArray[7]);
        log.info(" 500," + _latencyCountArray[8]);
        log.info(" 600," + _latencyCountArray[9]);
        log.info(" 700," + _latencyCountArray[10]);
        log.info(" 800," + _latencyCountArray[11]);
        log.info(" 900," + _latencyCountArray[12]);
        log.info("1000," + _latencyCountArray[13]);
        log.info("1+ms," + _latencyCountArray[14]);
    }
}
