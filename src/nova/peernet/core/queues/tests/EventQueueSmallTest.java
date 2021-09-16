package nova.peernet.core.queues.tests;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;

import nova.peernet.core.queues.ConcurrentBigHeap;
import peernet.config.Configuration;
import peernet.core.Event;
import peernet.core.EventQueue;
import peernet.core.Node;
import peernet.transport.Address;
import peernet.transport.AddressSim;

public class EventQueueSmallTest {

	private static EventQueue queue;
	private Random r;
	private static long insertions;

	public EventQueueSmallTest(String queueClass) throws FileNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		EventQueueSmallTest.queue = (EventQueue) Class.forName(queueClass).getDeclaredConstructor().newInstance();
		System.err.println("Loaded tested class: " + EventQueueSmallTest.queue.getClass().getCanonicalName());
		r = new Random(System.currentTimeMillis());
		EventQueueSmallTest.insertions = 0;
	}

	private void load() {
		byte b = 0;
		for(int j = 0; j < (Integer.MAX_VALUE / 2); j++) {
			long t = Math.abs(r.nextLong());
			EventQueueSmallTest.queue.add(t, null, null, b, Long.valueOf(t));
			EventQueueSmallTest.insertions++;
		}
	}

	private void unloadAndOutput() {
		Event ev = EventQueueSmallTest.queue.removeFirst();
		long t = ev.time;
		while(ev != null) {
			//out.println(ev.time + " " + ev.event);		
			if(ev.time < t) {
				System.err.println("Error in order.");
				System.exit(1);
			}
			t = ev.time;
			ev = EventQueueSmallTest.queue.removeFirst();
		}
	}

	public static void main (String[] args) {
		if(args.length != 1) {
			System.err.println("Incorrect number of arguments.");
			System.err.println("Usage: java EventQueueSmallTest <class name>");
			System.exit(1);
		}

		try {
			EventQueueSmallTest eqst = new EventQueueSmallTest(args[0]);
			long startTime = System.currentTimeMillis();
			eqst.load();
			long endLoad = System.currentTimeMillis();
			eqst.unloadAndOutput();
			long endUnload = System.currentTimeMillis();
			System.out.println("Class being tested: " + queue.getClass().getCanonicalName());
			System.out.println("Number of events: " + (Integer.MAX_VALUE / 2));
			System.out.println("Load time: " + (endLoad - startTime) + "ms");
			System.out.println("Unload time: " + (endUnload - endLoad) + "ms");
			System.out.println("Total time: " + (endUnload - startTime) + "ms");
		} catch (Exception e) {
			System.err.println("Was able to insert " + EventQueueSmallTest.insertions + " events. (max int = " + Integer.MAX_VALUE + ")");
			e.printStackTrace();
			System.exit(1);
		}

	}

}
