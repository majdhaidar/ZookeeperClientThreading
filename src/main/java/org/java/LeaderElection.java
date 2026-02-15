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

    /**
     * Closes the connection to the ZooKeeper server.
     * This method attempts to release the ZooKeeper resources
     * and gracefully shuts down the connection. If the operation
     * is interrupted, the exception is caught and its stack trace
     * is printed.
     */
    public void close() {
        try {
            zooKeeper.close();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Waits indefinitely for a notification to resume execution.
     * <p>
     * This method uses a synchronized block to acquire a lock on the ZooKeeper instance,
     * and the thread executing this method will be blocked until it is notified.
     * It is typically used to keep the application running until the ZooKeeper
     * session disconnects or another thread explicitly notifies it.
     *
     * @throws InterruptedException if the thread waiting on the ZooKeeper instance
     *                              is interrupted while waiting.
     */
    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }


    /**
     * Processes the events received from ZooKeeper.
     * This method is triggered whenever a watched event occurs in the ZooKeeper ensemble,
     * such as changes in connection state or changes to watched nodes.
     * <p>
     * The method handles:
     * - Connection events (None): Logs the connection status and handles disconnection notifications.
     * - Node-related events (NodeCreated, NodeDeleted, NodeDataChanged, NodeChildrenChanged): Currently, these are ignored.
     *
     * @param watchedEvent the event received from ZooKeeper. This contains details about
     *                     the nature of the event such as the type of event and its state.
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
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
            case NodeDeleted:
            case NodeDataChanged:
            case NodeChildrenChanged:
                break;
        }
    }
}
