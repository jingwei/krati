package krati.store;

import krati.core.segment.AddressFormat;

/**
 * InvalidAddressException.
 * 
 * @author jwu
 *
 */
public class InvalidAddressException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public InvalidAddressException() {
        super("Invalid address");
    }

    public InvalidAddressException(long addr) {
        super("Invalid address: " + addr);
    }
    
    public InvalidAddressException(long addr, AddressFormat addrFormat) {
        super("Invalid address: segment=" + addrFormat.getSegment(addr) + " offset=" + addrFormat.getOffset(addr));
    }
}
