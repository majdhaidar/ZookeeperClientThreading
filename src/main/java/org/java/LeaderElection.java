package org.java;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class LeaderElection implements Watcher {
    //ZOOKEEPER ADDRESS
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    //SESSION TIMEOUT TO WHEN TO CONSIDER A CONNECTED CLIENT DEAD
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void close(){
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    /**
     * This process method will be called by the zookeeper library
     *
     * @param watchedEvent
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Connected to Zookeeper");
                }else{
                    synchronized (zooKeeper){
                        System.out.println("Disconnected from Zookeeper");
                        zooKeeper.notifyAll();
                    }
                }
            case NodeCreated:
            case NodeDeleted:
            case NodeDataChanged:
            case NodeChildrenChanged:
                break;
        }
    }
}
