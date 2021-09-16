package nova.peernet.core.queues;

import java.util.Arrays;
import java.util.Random;

import peernet.core.Event;
import peernet.core.EventQueue;
import peernet.core.Events;
import peernet.core.Node;
import peernet.transport.Address;

/**
 * @author Margarida Mamede
 */


public class ConcurrentBigHeapWithInterval implements EventQueue {

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    // Default capacity of the extendable array.
    private static final int DEF_VEC_CAP = 16;     

    // Maximum capacity of the extendable array: 2^30
    private static final int MAX_VEC_CAP = 1073741824;     

    // The growth factor of the extendable array.
    private static final int GROWTH_FACTOR = 2;

    // Maximum capacity of the heap: 2^60
    public static final long MAX_CAPACITY = 
        ((long) MAX_VEC_CAP) * Slice.MAX_CAPACITY;     

    // Default capacity of the array with the events with minimum time.
    public static final int DEF_ARR_CAP = 1024;
    // Time window used to return multiple events together.
    public static final int DEF_NO_TIME_INTERFERENCE = 50;

    // -----------------------------------------------------------------------
    // Instance variables
    // -----------------------------------------------------------------------

    // Memory of the heap: an extendable array of slices.
    private Slice[] vec;

    // Number of slices in the array.
    private int vecSize;

    // Number of events in the heap.
    private long size;

    // The object used to return the events with minimum time.
    private Events minA;

    // The object used to return an event with minimum time.
    private Event minE; 

    // The event used to communicate with the slices.
    private Event last;


    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /** 
     * Creates a heap (with the default initial capacity) 
     * which returns an array with the default capacity (DEF_ARR_CAP)
     * when the method removeMany is called.
     */
    public ConcurrentBigHeapWithInterval( ) {
        this(DEF_ARR_CAP);
    }
    
    /** 
     * Creates a heap (with the default initial capacity) 
     * which returns an array with the specified capacity
     * when the method removeMany is called.
     *
     * @param capacity: the capacity of the array in the Events object
     *                  returned by the method removeMany.
     */
    public ConcurrentBigHeapWithInterval( int capacity ) {
        vec = new Slice[DEF_VEC_CAP];
        vecSize = 0;
        size = 0L;
        minA = new Events(capacity);
        minE = new Event();
        last = new Event();
    }





    // ------------------------------------------------------------------------
    // Public methods
    // ------------------------------------------------------------------------

    /** 
     * Returns true iff the heap contains no events.
     */
    public boolean isEmpty( ) { 
        return size == 0L;
    }


    /** 
     * Returns true iff the heap cannot contain more events.
     */
    public boolean isFull( ) {
        return size == MAX_CAPACITY;
    }


    /** 
     * Returns the number of events in the heap.
     */
    public long size( ) {
        return size;
    }


    /** 
     * Returns the minimum time of the events in the heap.
     * If there are no events in the heap, returns Long.MAX_VALUE.
     *
     * @return the smallest time of an event in the heap 
     *         or Long.MAX_VALUE (if the size is zero).
     */
    public long getNextTime( ) {
        if ( this.isEmpty() )
            return Long.MAX_VALUE;

        return vec[0].getKey(0);
    }


    /**
     * Inserts the specified event into the heap.
     * If the heap cannot contain one more event, 
     * a runtime exception is raised.
     * 
     * @param time:  the time at which this event should be scheduled
     * @param src:   ???
     * @param node:  the node at which the event has to be delivered
     * @param pid:   the protocol that handles the event
     * @param event: the object decribing the event
     */
    public synchronized void add( long time, Address src, Node node, byte pid, 
                     Object event ) {
        if ( this.isFull() )
            throw new RuntimeException("Heap is full."); 

        if ( vecSize == 0 || vec[vecSize - 1].isFull() )
            this.addSlice();
             
        long hole = size;
        Slice hSlc = vec[vecSize - 1];
        int hPos = (int) (hole % Slice.MAX_CAPACITY);
        hSlc.addLast(time, src, node, pid, event);
        // Percolate up.
        while ( hole > 0 ) {
            long parent = (hole - 1) / 2;
            Slice pSlc = vec[(int) (parent / Slice.MAX_CAPACITY)];
            int pPos = (int) (parent % Slice.MAX_CAPACITY);
            if ( time < pSlc.getKey(pPos) ) { 
                hSlc.copyEntry(pSlc, pPos, hPos);
                hole = parent;
                hSlc = pSlc;
                hPos = pPos;
            }
            else
                break;
        }
        if ( hole < size )
            hSlc.setEntry(hPos, time, src, node, pid, event);
        size++;
    }


    /**
     * Removes an event with the minimum time from the heap
     * and returns that event.
     * Notice that a singleton instance of the Event class is used. 
     * Therefore, the data contained in the returned object are overwritten 
     * when a new invocation of this method is performed.
     * If there are no events in the heap, returns null.
     * 
     * @return an event in the heap 
     *         or null (if the size is zero).
     */
    public synchronized Event removeFirst( ) {
        if ( this.isEmpty() ) 
            return null;
               
        this.remove(minE);
        return minE;
    }


    /**
     * Removes the events with the minimum time from the heap
     * and returns those events.
     * Notice that a singleton instance of the Events class is used. 
     * Therefore, the data contained in the returned object are overwritten 
     * when a new invocation of this method is performed.
     * The field size of the returned object contains 
     * the number of events removed from the heap. That number is:
     *   zero, if there are no events in the heap;
     *   the length of the array, if there are more events with the minimum
     *       time than the capacity of the array. 
     * 
     * @return some events in the heap 
     */
    public synchronized Events removeMany( ) {
        Event[] arr = minA.array;
        int max = (int) Math.min(arr.length, size);
        if ( max == 0 )
            minA.size = 0;
        else {
            Slice slc = vec[0];
            long minK = slc.getKey(0) + DEF_NO_TIME_INTERFERENCE;
            int c = 0;
            do  
                {
                    this.remove(arr[c]);
                    c++;
                }
            while ( c < max && slc.getKey(0) <= minK );
            minA.size = c;
        }
        return minA;        
    }


    // ------------------------------------------------------------------------
    // Private methods
    // ------------------------------------------------------------------------

    // Removes an event with the smallest time from the heap
    // and returns that event in the parameter res.
    //
    // Pre: !this.isEmpty()
    private void remove( Event res ) {
        vec[0].getEntry(0, res);
        vec[vecSize - 1].removeLast(last);
        if ( vec[vecSize - 1].isEmpty() )
            vecSize--;
        size--;
        if ( size == 1 )
            vec[0].setEntry(0, last);
        else
            this.percolateDown();  
    }


    // Establishes the heap order property 
    // when it holds from positions 1 to size - 1.
    //
    // Pre: size > 1.
    private void percolateDown( ) {
        long key = last.time;
        long hole = 0L;
        Slice hSlc = vec[0];
        int hPos = 0;
        long child = 2 * hole + 1;    // Left child.
        long other;
        Slice cSlc, oSlc;
        int cPos, oPos;
        long cKey, oKey;
        while ( child < size ) {
            // Find the smallest child.
            cSlc = vec[(int) (child / Slice.MAX_CAPACITY)];
            cPos = (int) (child % Slice.MAX_CAPACITY);
            cKey = cSlc.getKey(cPos);
            other = child + 1; 
            if ( other < size ) {
                oSlc = vec[(int) (other / Slice.MAX_CAPACITY)];
                oPos = (int) (other % Slice.MAX_CAPACITY);
                oKey = oSlc.getKey(oPos);
                if ( oKey < cKey ) {
                    child = other;
                    cSlc = oSlc;
                    cPos = oPos;
                    cKey = oKey;
                }
            }
            // Compare the smallest child with key.
            if ( cKey < key ) {
                hSlc.copyEntry(cSlc, cPos, hPos);
                hole = child; 
                hSlc = cSlc;
                hPos = cPos;
                child = 2 * hole + 1;    // Left child.
            }
            else
                break;
        }
        hSlc.setEntry(hPos, last);
    }


    // Pre: !this.isFull()
    private void addSlice( ) {
        if ( vecSize == vec.length ) {
            int cap = GROWTH_FACTOR * vecSize;
            Slice[] newVec = new Slice[cap];
            System.arraycopy(vec, 0, newVec, 0, vecSize);
            vec = newVec;
        }
        if ( vec[vecSize] == null )
            vec[vecSize] = new Slice();
        vecSize++;
    }
}
