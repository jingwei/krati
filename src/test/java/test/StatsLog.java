package test;

import org.apache.log4j.Logger;

/**
 * StatsLog
 * 
 * @author jwu
 * 
 */
public class StatsLog {
    public static final Logger logger = Logger.getLogger("krati.stats");
    
    static {
        logger.setAdditivity(false); // Do not add to ancestral loggers
    }
    
    public static void beginUnit(String unitTestName) {
        logger.info(">>> ========================================================");
        logger.info(">>> BEGIN " + unitTestName);
    }
    
    public static void endUnit(String unitTestName) {
        logger.info(">>> END " + unitTestName);
        logger.info(">>> ");
    }
}
