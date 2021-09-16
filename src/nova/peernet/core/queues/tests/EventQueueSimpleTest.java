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

public class EventQueueSimpleTest {

	private static EventQueue queue;
	private static int iterationsToLoad;
	//private BufferedOutputStream bout;
	//private PrintStream out;
	private Random r;
	private static long insertions;

	public EventQueueSimpleTest(String queueClass, int iterations /**, String outputfile**/) throws FileNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		EventQueueSimpleTest.queue = (EventQueue) Class.forName(queueClass).getDeclaredConstructor().newInstance();
		System.err.println("Loaded tested class: " + EventQueueSimpleTest.queue.getClass().getCanonicalName());
		EventQueueSimpleTest.iterationsToLoad = iterations;
		//bout = new BufferedOutputStream(new FileOutputStream(new File(outputfile)));
		//out = new PrintStream(bout);
		r = new Random(System.currentTimeMillis());
		EventQueueSimpleTest.insertions = 0;
	}

	private void load() {
		byte b = 0;

		for(int i = 0; i < EventQueueSimpleTest.iterationsToLoad; i++) {
			for(int j = 0; j < Integer.MAX_VALUE; j++) {
				long t = Math.abs(r.nextLong());
				EventQueueSimpleTest.queue.add(t, null, null, b, Long.valueOf(t));
				EventQueueSimpleTest.insertions++;
			}
		}
	}

	private void unloadAndOutput() {
		Event ev = EventQueueSimpleTest.queue.removeFirst();
		long t = ev.time;
		while(ev != null) {
			//out.println(ev.time + " " + ev.event);		
			if(ev.time < t) {
				System.err.println("Error in order.");
				System.exit(1);
			}
			t = ev.time;
			ev = EventQueueSimpleTest.queue.removeFirst();
		}
	}

	public static void main (String[] args) {
		if(args.length != 2 /**3**/) {
			System.err.println("Incorrect number of arguments.");
			System.err.println("Usage: java EventQueueTest <class name> <number of iterations> <output file>");
			System.exit(1);
		}

		try {
			EventQueueSimpleTest eqst = new EventQueueSimpleTest(args[0], Integer.parseInt(args[1])/**, args[2]**/);
			long startTime = System.currentTimeMillis();
			eqst.load();
			long endLoad = System.currentTimeMillis();
			eqst.unloadAndOutput();
			long endUnload = System.currentTimeMillis();
			System.out.println("Class being tested: " + queue.getClass().getCanonicalName());
			System.out.println("Number of events: " + iterationsToLoad + " x " + Integer.MAX_VALUE);
			System.out.println("Load time: " + (endLoad - startTime) + "ms");
			System.out.println("Unload time: " + (endUnload - endLoad) + "ms");
			System.out.println("Total time: " + (endUnload - startTime) + "ms");
		} catch (Exception e) {
			System.err.println("Was able to insert " + EventQueueSimpleTest.insertions + " events. (max int = " + Integer.MAX_VALUE + ")");
			e.printStackTrace();
			System.exit(1);
		}

	}

}
