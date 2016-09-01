/**
 * 
 */
package stormBench.stormBench.zookeeper;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.ZooKeeper;
import org.apache.storm.shade.org.apache.zookeeper.data.Stat;

/**
 * @author Roland
 *
 */
public class ZookeeperClient {

	private String host;
	private ZooKeeper zookeeper;
	private Watcher watcher;
	private String zNodeName = "/TestTopologyState";
	private byte[] state;
	private static Logger logger = Logger.getLogger("ZookeeperClient");
	
	public ZookeeperClient(String host, String id) {
		this.host = host;
		this.watcher = new WatcherImpl();
		this.zNodeName = "/" + id + "State";
		try {
			this.zookeeper = new ZooKeeper(this.host, 20000, this.watcher);
		} catch (IOException e) {
			logger.severe("Unable to communicate with the Zookeeper cluster because " + e);
		}
	}
	
	public void close(){
		try {
			this.zookeeper.close();
		} catch (InterruptedException e) {
			logger.severe("Unable to close Zookeeper connection because " + e);
		}
	}
	
	public Stat existsZNode(){
		Stat stat = null;
		try {
			stat = this.zookeeper.exists(this.zNodeName,  false);
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to check existence of zNode for state persistence because " + e);
		}
		return stat;
	}
	
	public void createZNode(){
		try {
			Stat stat = this.existsZNode();
			if(stat == null){
				this.zookeeper.create(this.zNodeName, this.state, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to create the zNode on " + this.host + " because " + e);
		}
	}
	
	public void persistState(byte[] state){
		this.state = state;
		try {
			Stat stat = this.existsZNode();
			if(stat != null){
				this.zookeeper.setData(this.zNodeName, this.state, stat.getVersion());
			}
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to persist a state in the zNode because " + e);
		}
	}

	public byte[] getState(){
		byte[] result = null;
		try {
			Stat stat = existsZNode();
			if(stat != null){
				result = this.zookeeper.getData(this.zNodeName, false, stat);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to recover data from the zNode because " + e);
		}
		return result;
	}
}