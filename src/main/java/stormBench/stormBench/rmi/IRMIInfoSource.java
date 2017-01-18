/**
 * 
 */
package stormBench.stormBench.rmi;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * @author Roland
 *
 */
public interface IRMIInfoSource extends Remote, Serializable {
	
	public ArrayList<String> getInfo() throws RemoteException;
	
	public void releaseRegistry() throws RemoteException;

}
