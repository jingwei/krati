package krati.cds.impl.array.entry;

public class EntryLongDualFactory implements EntryFactory<EntryValueLongDual>
{
    private int idCounter = 0;
    
    @Override
    public Entry<EntryValueLongDual> newEntry(int initialCapacity)
    {
        return new PreFillEntryLongDual(idCounter++, initialCapacity);
    } 
}
