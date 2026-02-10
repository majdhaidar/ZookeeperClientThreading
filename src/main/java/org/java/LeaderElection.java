package org.java;

import org.apache.zookeeper.*;

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

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
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

    public void close() {
        try {
            zooKeeper.close();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
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
