package krati.mds.impl.array;

public class CompactionAbortedException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    public CompactionAbortedException()
    {
        super("Compaction aborted");
    }
}