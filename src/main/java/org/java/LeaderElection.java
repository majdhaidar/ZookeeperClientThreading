package org.java;

import org.apache.zookeeper.*;

import java.io.IOException;

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
    public void volunteerForLeaderShip(){
        // c -> stands for candidate
        String zNodePrefix = ELECTION_NAMESPACE + "/c_";
        //The znode will be deleted upon the client's disconnect, and its name will be appended with a monotonically increasing number
        try {
            String zNodeFullPath = zooKeeper.create(zNodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("Volunteered for leadership: "+zNodeFullPath);
            //extracting current znode name from full path
            currentZnodeName = zNodeFullPath.substring(zNodeFullPath.lastIndexOf("/")+1);
        }
        catch (KeeperException e) {
            System.err.println("Error while creating znode: "+zNodePrefix);
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            System.err.println("Interrupted while creating znode: "+zNodePrefix);
            throw new RuntimeException(e);
        }
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
