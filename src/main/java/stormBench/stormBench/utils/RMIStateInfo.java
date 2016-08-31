/**
 * 
 */
package stormBench.stormBench.utils;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * @author Roland
 *
 */
public class RMIStateInfo extends UnicastRemoteObject implements IRMIStateInfo {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7025297173948782330L;
	private Integer index;
	
	public RMIStateInfo(Integer index) throws RemoteException {
		super();
		this.index = index;
	}

	/* (non-Javadoc)
	 * @see stormBench.stormBench.utils.IRMIStateInfo#getIndex()
	 */
	@Override
	public Integer getIndex() throws RemoteException {
		return this.index;
	}

}
