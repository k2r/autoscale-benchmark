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
	private String zNodeState;
	private String zNodeDate;
	private byte[] state;
	private byte[] date;
	private static Logger logger = Logger.getLogger("ZookeeperClient");
	
	public ZookeeperClient(String host, String id) {
		this.host = host;
		this.watcher = new WatcherImpl();
		this.zNodeState = "/" + id + "State";
		this.zNodeDate = "/" + id + "LastEmissionDate";
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
	
	public Stat existsZNodeState(){
		Stat stat = null;
		try {
			stat = this.zookeeper.exists(this.zNodeState,  false);
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to check existence of zNode for state persistence because " + e);
		}
		return stat;
	}
	
	public Stat existsZNodeDate(){
		Stat stat = null;
		try {
			stat = this.zookeeper.exists(this.zNodeDate,  false);
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to check existence of zNode for last emission persistence because " + e);
		}
		return stat;
	}
	
	public void createZNodeState(){
		try {
			Stat stat = this.existsZNodeState();
			if(stat == null){
				this.zookeeper.create(this.zNodeState, this.state, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to create the zNode on " + this.host + " because " + e);
		}
	}
	
	public void createZNodeDate(){
		try {
			Stat stat = this.existsZNodeDate();
			if(stat == null){
				this.zookeeper.create(this.zNodeDate, this.date, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to create the zNode on " + this.host + " because " + e);
		}
	}
	
	public void persistState(byte[] state){
		this.state = state;
		try {
			Stat stat = this.existsZNodeState();
			if(stat != null){
				this.zookeeper.setData(this.zNodeState, this.state, stat.getVersion());
			}
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to persist a state in the zNode because " + e);
		}
	}
	
	public void persistDate(byte[] date){
		this.date = date;
		try {
			Stat stat = this.existsZNodeDate();
			if(stat != null){
				this.zookeeper.setData(this.zNodeDate, this.date, stat.getVersion());
			}
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to persist a last emission date in the zNode because " + e);
		}
	}

	public byte[] getState(){
		byte[] result = null;
		try {
			Stat stat = existsZNodeState();
			if(stat != null){
				result = this.zookeeper.getData(this.zNodeState, false, stat);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to recover data from the zNode because " + e);
		}
		return result;
	}
	
	public byte[] getDate(){
		byte[] result = null;
		try {
			Stat stat = existsZNodeDate();
			if(stat != null){
				result = this.zookeeper.getData(this.zNodeDate, false, stat);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.severe("Unable to recover data from the zNode because " + e);
		}
		return result;
	}
}