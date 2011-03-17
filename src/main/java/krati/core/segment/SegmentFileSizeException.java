package krati.core.segment;

/**
 * SegmentFileSizeException
 * 
 * @author jwu
 * 
 */
public class SegmentFileSizeException extends SegmentException {
    private final static long serialVersionUID = 1L;
    private final String segFilePath;
    private final int segFileSizeMB;
    private final int expFileSizeMB;

    public SegmentFileSizeException(String segmentFilePath, int segmentFileSizeMB, int expectedFileSizeMB) {
        super("Invalid segment file size in MB: " + segmentFileSizeMB);
        this.segFilePath = segmentFilePath;
        this.segFileSizeMB = segmentFileSizeMB;
        this.expFileSizeMB = expectedFileSizeMB;
    }

    public String getSegmentFilePath() {
        return segFilePath;
    }

    public int getSegmentFileSizeMB() {
        return segFileSizeMB;
    }

    public int getExpectedFileSizeMB() {
        return expFileSizeMB;
    }
}
