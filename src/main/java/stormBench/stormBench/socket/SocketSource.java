/**
 * 
 */
package stormBench.stormBench.socket;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * @author Roland
 *
 */
public class SocketSource implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 448781249463536262L;
	ServerSocket server;
	Socket client;
	private static final Logger logger = Logger.getLogger("SocketSource");
	
	public SocketSource(int port){
		try {
			this.server = new ServerSocket(port);
			this.client = server.accept();
		} catch (IOException e) {
			logger.severe("Unable to instanciate a new server on port " + port + " because " + e);
		}
	}
	
	public void sendBatch(ArrayList<String> batch){
		try {
			DataOutputStream out = new DataOutputStream(this.client.getOutputStream());
			int length = batch.size();
			for(int i = 0; i < length; i++){
				out.writeUTF(batch.get(i) + "\n");				
			}
			out.flush();
		} catch (IOException e) {
			logger.severe("Unable to send a batch because " + e);
		}
	}

	public void close(){
		try {
			this.client.close();
			this.server.close();
		} catch (IOException e) {
			logger.severe("Unable to close the source because " + e);
		}
		
	}
}
