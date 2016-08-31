/**
 * 
 */
package stormBench.stormBench.zookeeper;

import java.util.concurrent.CountDownLatch;

import org.apache.storm.shade.org.apache.zookeeper.WatchedEvent;
import org.apache.storm.shade.org.apache.zookeeper.Watcher;
import org.apache.storm.shade.org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * @author Roland
 *
 */
public class WatcherImpl implements Watcher {
	
	private final CountDownLatch connectedSignal = new CountDownLatch(1);

	/* (non-Javadoc)
	 * @see org.apache.storm.shade.org.apache.zookeeper.Watcher#process(org.apache.storm.shade.org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(WatchedEvent arg0) {
		 if (arg0.getState() == KeeperState.SyncConnected) {
             connectedSignal.countDown();
          }
	}
	
	public CountDownLatch getConectedSignal(){
		return this.connectedSignal;
	}

}
