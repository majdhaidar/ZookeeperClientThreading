package org.java;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class WatcherDemo implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static final String TARGET_ZNODE = "/target_znode";

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("Watcher event received: " + watchedEvent);
        switch (watchedEvent.getType()) {
            case None:
                System.out.println();
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Connected to Zookeeper");
                }
                else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper");
                        zooKeeper.notifyAll();
                    }
                }
            case NodeCreated:
                System.out.println("Target znode created: " + TARGET_ZNODE);
                break;
            case NodeDeleted:
                System.out.println("Target znode deleted: " + TARGET_ZNODE);
                break;
            case NodeDataChanged:
                System.out.println("Target znode data changed: " + TARGET_ZNODE);
                break;
            case NodeChildrenChanged:
                System.out.println("Target znode children changed: " + TARGET_ZNODE);
                break;
            default:
                System.out.println("Invalid event type: " + watchedEvent.getType());
                break;
        }
        System.out.println();
        watchTargetZnode();
        System.out.println();
    }

    public void watchTargetZnode() {
        System.out.println();
        try {
            System.out.println("Watching target znode: " + TARGET_ZNODE);
            Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
            if(null == stat){
                System.out.println("Target znode does not exist yet.");
                return;
            }
            byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
            System.out.println("Target znode data: " + new String(data));
            List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);
            System.out.println("Target znode children: " + children);

        }
        catch (KeeperException e) {
            System.err.println("[watchTargetZnode] Error while watching target znode.");
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            System.err.println("[watchTargetZnode] Interrupted while watching target znode.");
            throw new RuntimeException(e);
        }
        System.out.println("Target znode is now watched.");
    }

    public void run() {
        synchronized (zooKeeper) {
            try {
                zooKeeper.wait();
            }
            catch (InterruptedException e) {
                System.err.println("[run] Interrupted while waiting for Zookeeper connection.");
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        try {
            zooKeeper.close();
        }
        catch (InterruptedException e) {
            System.err.println("[close] Interrupted while closing Zookeeper connection.");
            throw new RuntimeException(e);
        }
    }

    public void connectToZookeeper() throws IOException {
        System.out.println("Connecting to Zookeeper...");
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        System.out.println("Connected to Zookeeper.");
    }

    public void createFirstTargetNode() throws InterruptedException, KeeperException {
        System.out.println("Creating target znode namespace...");
        if (zooKeeper.exists(TARGET_ZNODE, false) == null) {
            try {
                zooKeeper.create(
                        TARGET_ZNODE,
                        new byte[] {},
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT
                );
                System.out.println("Created target znode namespace: " + TARGET_ZNODE);
            }
            catch (KeeperException.NodeExistsException ignore) {
                // If another client created it in the meantime, it’s fine
                System.err.println("Target znode namespace already exists: " + TARGET_ZNODE + "");
                System.err.println(ignore);
            }
        }
        System.out.println("Target znode namespace already exists.");
    }
}
