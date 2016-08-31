/**
 * 
 */
package stormBench.stormBench.utils;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author Roland
 *
 */
public interface IRMIStateInfo extends Remote {

	public Integer getIndex() throws RemoteException;
}
