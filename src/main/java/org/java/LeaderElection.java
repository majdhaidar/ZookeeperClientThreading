package org.java;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    //ZOOKEEPER ADDRESS
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    //SESSION TIMEOUT TO WHEN TO CONSIDER A CONNECTED CLIENT DEAD
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodeName;
    private static final String TARGET_ZNODE = "/target_znode";


    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void watchTargetZnode() {
        try {
            Stat exists = zooKeeper.exists(TARGET_ZNODE, this);
            if (null == exists) {
                return;
            }
            System.out.println("Target znode exists. Watching it for changes.");
            byte[] data = zooKeeper.getData(TARGET_ZNODE, this, exists);
            System.out.println("Data read from znode: " + new String(data));
            List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);
            if (!children.isEmpty()) {
                System.out.println("Children of znode: " + children);
            }
        }
        catch (KeeperException e) {
            System.err.println("KeeperException: " + e);
        }
        catch (InterruptedException e) {
            System.err.println("InterruptedException: " + e);
        }

    }

    /**
     * new method created to handle node volunteer to leadership
     *
     */
    public void volunteerForLeaderShip() {
        // c -> stands for candidate
        String zNodePrefix = ELECTION_NAMESPACE + "/c_";
        //The znode will be deleted upon the client's disconnect, and its name will be appended with a monotonically increasing number
        try {

            // Ensure parent election znode exists
            if (zooKeeper.exists(ELECTION_NAMESPACE, false) == null) {
                try {
                    zooKeeper.create(
                            ELECTION_NAMESPACE,
                            new byte[] {},
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT
                    );
                    System.out.println("Created election namespace: " + ELECTION_NAMESPACE);
                }
                catch (KeeperException.NodeExistsException ignore) {
                    // If another client created it in the meantime, it’s fine
                    System.err.println("Election namespace already exists: " + ELECTION_NAMESPACE + "");
                    throw new RuntimeException(ignore);
                }
            }

            String zNodeFullPath = zooKeeper.create(zNodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("Volunteered for leadership: " + zNodeFullPath);
            //extracting current znode name from full path
            currentZnodeName = zNodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
        }
        catch (KeeperException e) {
            System.err.println("Error while creating znode: " + zNodePrefix);
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            System.err.println("Interrupted while creating znode: " + zNodePrefix);
            throw new RuntimeException(e);
        }
    }

    /**
     * Elects a leader among the participating nodes in the Zookeeper cluster
     * by identifying the znode with the smallest sequential number under the
     * specified election namespace.
     * <br>
     * The method retrieves the list of child znodes under the election namespace,
     * sorts them to determine the smallest znode, and compares it with the current node's
     * identifier to decide if the current node is the leader.
     * <br>
     * If the current node is the leader, it announces itself as the leader; otherwise,
     * it steps down and announces the smallest child as the leader.
     *
     * @throws InterruptedException if the thread is interrupted during execution
     * @throws RuntimeException     if an error occurs while interacting with Zookeeper
     */
    public void electLeader() throws InterruptedException {
        try {
            //returns list of znode children names under the provided namespace
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            //the list returns unsorted list, to get the smallest child we will need to sort it
            Collections.sort(children);
            String smallestChild = children.get(0);
            System.out.println("Elected leader: " + smallestChild);
            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader, I will continue to serve");
                return;
            }
            System.out.println("I am not the leader, I will step down. The leader is: " + smallestChild + "");
        }
        catch (KeeperException e) {
            System.err.println("Error while getting children of " + ELECTION_NAMESPACE);
            throw new RuntimeException(e);
        }
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
        System.out.println("Event received: " + watchedEvent.getType());
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
                System.out.println("Node created: " + watchedEvent.getPath());
                break;
            case NodeDeleted:
                System.out.println("Node deleted: " + watchedEvent.getPath());
            case NodeDataChanged:
                System.out.println(">" + watchedEvent.getPath() + "");
            case NodeChildrenChanged:
                System.out.println("Children changed for node: " + watchedEvent.getPath());
                break;
        }
        watchTargetZnode();

    }
}
