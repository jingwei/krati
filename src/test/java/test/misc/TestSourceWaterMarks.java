package test.misc;

import java.io.File;
import java.io.IOException;

import test.util.FileUtils;

import junit.framework.TestCase;
import krati.util.SourceWaterMarks;

/**
 * TestSourceWaterMarks
 * 
 * @author jwu
 * 03/04, 2011
 * 
 */
public class TestSourceWaterMarks extends TestCase {
    protected File scnFile;

    @Override
    protected void setUp() {
        scnFile = new File(FileUtils.getTestDir(getClass().getSimpleName()), "sourceWaterMarks.scn");
        if (scnFile.exists()) {
            scnFile.delete();
        }
    }

    @Override
    protected void tearDown() {
        File dir = FileUtils.getTestDir(getClass().getSimpleName());
        if (dir.exists()) {
            try {
                FileUtils.deleteDirectory(dir);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void testApiBasics() {
        int counter = 1;
        SourceWaterMarks swm = new SourceWaterMarks(scnFile);
        swm.syncWaterMarks();

        String source1 = "database.source1";
        long scn = System.currentTimeMillis();
        swm.setLWMScn(source1, scn);
        swm.setHWMScn(source1, scn);
        assertEquals(swm.getHWMScn(source1), swm.getLWMScn(source1));

        scn = System.currentTimeMillis() + counter++;
        swm.saveHWMark(source1, scn);
        assertTrue(swm.getLWMScn(source1) < swm.getHWMScn(source1));

        scn = System.currentTimeMillis() + counter++;
        swm.setWaterMarks(source1, scn, scn);
        assertEquals(scn, swm.getLWMScn(source1));
        assertEquals(swm.getLWMScn(source1), swm.getHWMScn(source1));

        scn = System.currentTimeMillis() + counter++;
        swm.saveHWMark(source1, scn);
        assertTrue(swm.getLWMScn(source1) < swm.getHWMScn(source1));

        swm.syncWaterMarks();
        assertEquals(swm.getHWMScn(source1), swm.getLWMScn(source1));

        String source2 = "database.source2";
        scn = System.currentTimeMillis() + counter++;
        swm.setLWMScn(source2, scn);
        swm.setHWMScn(source2, scn);
        assertEquals(swm.getHWMScn(source2), swm.getLWMScn(source2));

        assertEquals(2, swm.sources().size());

        scn = System.currentTimeMillis() + counter++;
        swm.saveHWMark(source1, scn);
        swm.saveHWMark(source2, scn);
        assertTrue(swm.getLWMScn(source1) < swm.getHWMScn(source1));
        assertTrue(swm.getLWMScn(source2) < swm.getHWMScn(source2));

        swm.syncWaterMarks();
        assertEquals(swm.getHWMScn(source1), swm.getLWMScn(source1));
        assertEquals(swm.getHWMScn(source2), swm.getLWMScn(source2));
        assertTrue(swm.getFile().exists());
        assertTrue(swm.getFileOriginal().exists());

        SourceWaterMarks swm1 = new SourceWaterMarks(swm.getFile());
        assertEquals(swm.getLWMScn(source1), swm1.getLWMScn(source1));
        assertEquals(swm.getHWMScn(source1), swm1.getHWMScn(source1));
        assertEquals(swm.getLWMScn(source2), swm1.getLWMScn(source2));
        assertEquals(swm.getHWMScn(source2), swm1.getHWMScn(source2));

        // make file and fileOriginal have the same water marks
        swm.syncWaterMarks();

        SourceWaterMarks swm2 = new SourceWaterMarks(swm.getFileOriginal());
        assertEquals(swm.getLWMScn(source1), swm2.getLWMScn(source1));
        assertEquals(swm.getHWMScn(source1), swm2.getHWMScn(source1));
        assertEquals(swm.getLWMScn(source2), swm2.getLWMScn(source2));
        assertEquals(swm.getHWMScn(source2), swm2.getHWMScn(source2));

        swm.clear();
        assertFalse(swm.getFileOriginal().exists());
        assertEquals(0, new SourceWaterMarks(swm.getFile()).sources().size());
    }
}
