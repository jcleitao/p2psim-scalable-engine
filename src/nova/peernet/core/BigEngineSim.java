/*
 * Created on Apr 28, 2012 by Spyros Voulgaris
 *
 */
package nova.peernet.core;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import peernet.Simulator;
import peernet.config.Configuration;
import peernet.core.CommonState;
import peernet.core.Engine;
import peernet.core.Event;
import peernet.core.EventQueue;
import peernet.core.Events;
import peernet.core.Heap;
import peernet.core.Node;
import peernet.core.Protocol;
import peernet.core.Schedule;
import peernet.transport.Address;


public class BigEngineSim extends Engine
{

	EventQueue eventQueue = null;
	ExecutorService threadPool = Executors.newCachedThreadPool();
	List<Event> pendingControlEvents = new ArrayList<Event>();
	Map<Node,Future<?>> pendingTasks = new HashMap<Node,Future<?>>(1024); //Fix this constant

	@Override
	public void startExperiment()
	{
		super.startExperiment();

		// Perform the actual simulation; executeNext() will tell when to stop.
		boolean exit = false;
		Events evs;
		//long events_processed = 0;
		while (!exit) {
			evs = eventQueue.removeMany();
			if(evs.size > 1) {
				//System.out.print(evs.size);
				exit = executeNextEvents(evs);
				waitForTermination();
				exit = exit || executePendingControlEvents();	
			} else if(evs.size == 1){
				//System.out.print(1);
				exit = executeNextEvent(evs.array[0]);
			} else {
				//System.out.print(0);
				System.err.println("Engine: queue is empty, quitting" + " at time " + CommonState.getTime());
				exit = true;
			}
			/**events_processed += evs.size;
			if(events_processed >= 100) {
				System.out.println(" [" + events_processed + "] Time: " + CommonState.getTime());
				events_processed = 0;
			}**/

		}

		// analysis after the simulation
		//CommonState.setPhase(CommonState.POST_SIMULATION);
		for (int j = 0; j<controls.length; ++j)
		{
			if (controlSchedules[j].fin)
				controls[j].execute();
		}
	}

	private boolean executePendingControlEvents() {
		boolean ret = false; //Return value (false means continue)

		if(!pendingControlEvents.isEmpty()) {

			for(Event ev: this.pendingControlEvents) {
				int pid = ev.pid;
				ret = ret || controls[pid].execute();
				long delay = controlSchedules[pid].nextDelay(ev.time);
				if (delay>=0)
					addEventIn(delay,  null, null, pid, null);
			}

			pendingControlEvents.clear();
		}

		return ret;
	}

	private void waitForTermination() {
		for(Future<?> f: pendingTasks.values()) {
			while(true) {
				try {		
					f.get();
					break;	
				} catch (InterruptedException e) {
					;
				} catch (ExecutionException e) {
					System.err.println("Execution of simulated task failed.");
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		pendingTasks.clear();
	}

	private void processNodeEvent(Event ev, int pid, long time) {
		//      CommonState.setPid(pid);  //NOTE: try to entirely avoid CommonState
		//      CommonState.setNode(ev.node);
		if (ev.event instanceof Schedule)
		{
			Protocol prot = ev.node.getProtocol(pid);
			prot.nextCycle(((Schedule)ev.event).schedId);

			long delay = prot.nextDelay();
			if (delay == 0)
				delay = ((Schedule)ev.event).nextDelay(time);

			if (delay > 0)
				addEventIn(delay, null, ev.node, pid, ev.event);
		}
		else // call Protocol.processEvent()
		{
			Protocol prot = ev.node.getProtocol(pid);
			prot.processEvent(ev.src, ev.event);
		}
	}


	/**
	 * Execute and remove the next event without using threads.
	 */
	private boolean executeNextEvent(Event ev) {

		long time = ev.time; // >> rbits;
		//System.err.println(ev.time + " " + ev.pid + " " + ev.node + " " + ev.src + " " + ev.event);
		//System.err.println("Removing event at time: " + time);
		if (time >= nextlog) {
			System.err.println("Current time: " + time);
			do {
				nextlog += logtime;
			}
			while (time >= nextlog);
		}
		if (time >= endtime) {
			System.err.println("Engine: reached end time, quitting, leaving " + eventQueue.size() + " unprocessed events in the queue");
			return true;
		}
		CommonState.setTime(time);
		int pid = ev.pid;
		if (ev.node == null)  //XXX: Not an elegant way to identify control events
		{
			boolean ret = controls[pid].execute();
			long delay = controlSchedules[pid].nextDelay(time);
			if (delay >= 0)
				addEventIn(delay, null, null, pid, null);
			return ret;
		} else if (ev.node.isUp()) {
			//      CommonState.setPid(pid);  // XXX try to entirely avoid CommonState
			//      CommonState.setNode(ev.node);
			if (ev.event instanceof Schedule) {
				Protocol prot = ev.node.getProtocol(pid);
				prot.nextCycle(((Schedule) ev.event).schedId);

				long delay = prot.nextDelay();
				if (delay == 0)
					delay = ((Schedule) ev.event).nextDelay(time);

				if (delay > 0)
					addEventIn(delay, null, ev.node, pid, ev.event);
			} else // call Protocol.processEvent()
			{
				Protocol prot = ev.node.getProtocol(pid);
				prot.processEvent(ev.src, ev.event);
			}
		}
		return false;
	}

	/**
	 * Execute and remove the next event from the ordered event list.
	 * 
	 * @return true if the execution should be stopped.
	 */
	private boolean executeNextEvents(Events evs)
	{
		Event ev = null;

		for(int i = 0; i < evs.size; i++) {
			ev = evs.array[i];
			long time = ev.time;//>>rbits;
			//System.err.println("Removing event at time: " + time);

			if (time>=nextlog)
			{
				System.err.println("Current time: "+time);
				do
				{
					nextlog += logtime;
				}
				while (time>=nextlog);
			}
			if (time>=endtime)
			{
				System.err.println("Engine: reached end time, quitting, leaving "+ eventQueue.size() +" unprocessed events in the queue");
				return true;
			}

			CommonState.setTime(time);

			//System.err.println("Sequencing over events: " + i + "/" + evs.size);
			if (ev.node == null)  //TODO: Fix this not an elegant way to identify control events
			{
				//System.err.println("Control event added to internal queue");
				pendingControlEvents.add(ev);			
			}
			else if (ev.node.isUp())
			{
				//System.err.println("Check future existence for target node");
				int pid = ev.pid;
				Future<?> f = pendingTasks.get(ev.node);
				if(f != null) {
					while(true) { 
						try {
							f.get();
							break;
						} catch (InterruptedException e) {
							;
						} catch (ExecutionException e) {
							System.err.println("Execution of simulated task failed.");
							e.printStackTrace();
							System.exit(1);
						}		
					}
				}
				//System.err.println("Scheduling new task");
				final Event e = ev;
				pendingTasks.put(ev.node, threadPool.submit(new Runnable() {

					@Override
					public void run() {
						processNodeEvent(e, pid, time);
					}

				}));

			}
		}
		//System.err.println("Teminating sequence");
		return false;
	}

	public void addEventAt(long time, Address src, Node node, int pid, Object event)
	{
		//time = (time<<rbits) | CommonState.r.nextInt(1<<rbits);
		//System.err.println("Adding event at time: " + time);
		eventQueue.add(time, src, node, (byte) pid, event);
	}

	@Override
	protected void createHeaps()
	{

		if(Configuration.contains(Simulator.PAR_SIM_HEAP)) {
			try {
				eventQueue = (EventQueue) Class.forName(Configuration.getString(Simulator.PAR_SIM_HEAP)).getDeclaredConstructor().newInstance();
				System.err.println("Loaded HEAP: " + Configuration.getString(Simulator.PAR_SIM_HEAP));
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
					| SecurityException | ClassNotFoundException | NoSuchMethodException e) {
				System.err.println("Could not instanciate Event Queue: " + Configuration.getString(Simulator.PAR_SIM_HEAP));
				e.printStackTrace();
				System.exit(1);
			}
		} else {
			eventQueue = new Heap();
		}	
	}

	public long pendingEvents()
	{
		return eventQueue.size();
	}



	@Override
	public void blockingInitializerStart()
	{
		throw new RuntimeException("Blocking initializers not applicable to SIM mode");
	}



	@Override
	public void blockingInitializerDone()
	{
		throw new RuntimeException("Blocking initializers not applicable to SIM mode");
	}
}
