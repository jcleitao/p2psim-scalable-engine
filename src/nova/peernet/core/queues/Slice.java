package nova.peernet.core.queues;

import peernet.core.Event;
import peernet.core.Node;
import peernet.transport.Address;

class Slice {

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    // Default capacity of the slice: 2^16.
    private static final int DEFAULT_CAPACITY = 65536;     

    // Maximum capacity of the slice: 2^30
    public static final int MAX_CAPACITY = 1073741824;     

    // The growth factor of the extendable array.
    private static final int GROWTH_FACTOR = 2;


    // -----------------------------------------------------------------------
    // Instance variables
    // -----------------------------------------------------------------------

    // Memory of the slice: 5 extendable arrays.
    private long[] times;
    private Address[] srcs;
    private Node[] nodes;
    private byte[] pids;
    private Object[] events;

    // Number of events in the slice.
    private int size;


    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    // Creates a slice with the specified capacity.
    public Slice( int capacity ) {
        times = new long[capacity];
        srcs = new Address[capacity];
        nodes = new Node[capacity];
        pids = new byte[capacity];
        events = new Object[capacity];
        size = 0;
    }


    // Creates a slice with the default capacity.
    public Slice( ) {
        this(DEFAULT_CAPACITY);
    }


    // ------------------------------------------------------------------------
    // Public methods
    // ------------------------------------------------------------------------

    // Returns true iff the slice contains no events.
    public boolean isEmpty( ) { 
        return size == 0;
    }


    // Returns true iff the slice cannot contain more events.
    public boolean isFull( ) {
        return size == MAX_CAPACITY;
    }


    // Returns the key at the specified position.
    // Pre: 0 <= pos < size
    public long getKey( int pos ) {
        return times[pos];
    }


    // Returns the event at the specified position (in the parameter ev).
    // Pre: 0 <= pos < size
    public void getEntry( int pos, Event ev ) {
        ev.time = times[pos];
        ev.src = srcs[pos];
        ev.node = nodes[pos];
        ev.pid = pids[pos];
        ev.event = events[pos];
    }


    // Updates the event at the specified position.
    // Pre: 0 <= pos < size
    public void setEntry( int pos, long time, Address src, Node node, 
                          byte pid, Object event ) {
        times[pos] = time;
        srcs[pos] = src;
        nodes[pos] = node;
        pids[pos] = pid;
        events[pos] = event;
    }


    // Updates the event at the specified position.
    // Pre: 0 <= pos < size
    public void setEntry( int pos, Event ev ) {
        times[pos] = ev.time;
        srcs[pos] = ev.src;
        nodes[pos] = ev.node;
        pids[pos] = ev.pid;
        events[pos] = ev.event;
    }


    // Copies the event at position idx of slice slc to position pos.
    // Pre: slc != null && 0 <= idx < slc.size && 0 <= pos < size
    public void copyEntry( Slice slc, int idx, int pos ) {
        times[pos] = slc.times[idx];
        srcs[pos] = slc.srcs[idx];
        nodes[pos] = slc.nodes[idx];
        pids[pos] = slc.pids[idx];
        events[pos] = slc.events[idx];
    }


    // Inserts the event at the last position of the slice.
    // Pre: !this.isFull()
    public void addLast( long time, Address src, Node node, byte pid, 
                         Object event ) {
        if( size > times.length )
        	System.err.println("addLast size: " + size + " times.length: " + times.length);
        if( times.length != srcs.length) {
        	System.err.println("times.length != srcs.length " + times.length + " " + srcs.length);
        }
        try {
    	if ( size == times.length )
            this.grow();
        times[size] = time;
        srcs[size] = src;
        nodes[size] = node;
        pids[size] = pid;
        events[size] = event;
        size++;
        } catch (Exception e) {
        	System.err.println("size: " + size);
        	System.err.println("times.lenght: " + times.length);
        	System.err.println("srcs.lenght: " + srcs.length);
        	System.err.println("nodes.lenght: " + nodes.length);
        	System.err.println("pids.lenght: " + pids.length);
        	System.err.println("events.lenght: " + events.length);
        	throw e;
        }
    }


    // Removes the last event from the slice 
    // (and returns it in parameter ev).
    // Pre: !this.isEmpty()
    public void removeLast( Event ev ) {
        size--;
        ev.time = times[size];
        // times[size] = 0L;
        ev.src = srcs[size];
        srcs[size] = null;
        ev.node = nodes[size];
        nodes[size] = null;
        ev.pid = pids[size];
        // pids[size] = 0;
        ev.event = events[size];
        events[size] = null;
    }


    // ------------------------------------------------------------------------
    // Private methods
    // ------------------------------------------------------------------------

    // Pre: !this.isFull()
    private void grow( ) {
        int cap = GROWTH_FACTOR * size;

        long[] newT = new long[cap];
        System.arraycopy(times, 0, newT, 0, size);
        times = newT;

        Address[] newS = new Address[cap];
        System.arraycopy(srcs, 0, newS, 0, size);
        srcs = newS;

        Node[] newN = new Node[cap];
        System.arraycopy(nodes, 0, newN, 0, size);
        nodes = newN;

        byte[] newP = new byte[cap];
        System.arraycopy(pids, 0, newP, 0, size);
        pids = newP;

        Object[] newE = new Object[cap];
        System.arraycopy(events, 0, newE, 0, size);
        events = newE;
    }
    
}