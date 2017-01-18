/**
 * 
 */
package stormBench.stormBench.rmi;

import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.logging.Logger;
import stormBench.stormBench.rmi.IRMIInfoSource;

/**
 * @author Roland
 *
 */
public class RMIInfoSource extends UnicastRemoteObject implements IRMIInfoSource {

	ArrayList<String> info;
	
	private int port;
	private Registry registry;
	private static final Logger logger = Logger.getLogger("RMIInfoSource");
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5844687925266614589L;

	public RMIInfoSource(int port) throws RemoteException {
		super(port);
		this.port = port;
		this.registry = LocateRegistry.createRegistry(this.port);
		if(registry != null){
			System.out.println("New RMIInfoSource created on port " + port);
		}
		this.info = new ArrayList<>();
	}

	@Override
	public ArrayList<String> getInfo() throws RemoteException{
		return this.info;
	}
	
	public void setInfo(ArrayList<String> info){
		this.info = info;
	}
	
	public void cast(){
		try {
			registry.bind("tuples", (IRMIInfoSource) this);
			Thread.sleep(20);
		} catch (RemoteException | InterruptedException e) {
			System.out.println("Server unable to bind the remote object because " + e);	
		} catch (AlreadyBoundException e) {
			try {
				registry.rebind("tuples", (IRMIInfoSource) this);
				Thread.sleep(200);
			} catch (RemoteException | InterruptedException e1) {
				System.out.println("Server unable to rebind the remote object because " + e1);
			}
		}
	}
	
	@Override
	public void releaseRegistry() throws RemoteException{
		try {
			UnicastRemoteObject.unexportObject(registry, true);
		} catch (NoSuchObjectException e) {
			logger.severe("There is no registry to release on given host/port");
		}
	}
}
