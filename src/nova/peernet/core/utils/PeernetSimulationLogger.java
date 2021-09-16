package nova.peernet.core.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import peernet.Simulator;
import peernet.config.Configuration;

public class PeernetSimulationLogger extends PrintStream {

	public PeernetSimulationLogger() throws FileNotFoundException {
		super(new File(Configuration.getString(Simulator.PAR_SIM_NAME) + ".log"));
	}
	
	public PeernetSimulationLogger(String filename) throws FileNotFoundException {
		super(new File(Configuration.getString(Simulator.PAR_SIM_NAME) + ".log"));
		System.err.println("Activating logger with input: " + filename);
	}
}
