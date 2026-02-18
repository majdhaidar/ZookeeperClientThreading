package org.java;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Hello Zookeeper!");
        switch (args[0]){
            case "leadereletction":
                    leaderElectionDemon();
                break;
            case "watcherdemo":
                    watcherDemo();
                break;
            default:
                System.out.println("Invalid argument");
        }
    }

    private static void watcherDemo() {
    }

    /**
     * Initializes and orchestrates the leader election process in a Zookeeper cluster.
     * This method performs the following steps:
     * 1. Establishes a connection to the Zookeeper service.
     * 2. Submits the current node as a candidate for leadership by creating an ephemeral sequential znode.
     * 3. Executes the leader election process to determine if the current node is the leader or a participant.
     * 4. Waits indefinitely to maintain the Zookeeper session and to observe Zookeeper events, enabling
     *    the node to respond to leadership changes or other cluster activities.
     * 5. Closes the Zookeeper connection upon exiting the process.
     *
     * If any step fails, appropriate exceptions are propagated.
     *
     * @throws IOException           if an error occurs while connecting to Zookeeper.
     * @throws InterruptedException  if the thread is interrupted during any blocking operation.
     */
    private static void leaderElectionDemon() throws IOException, InterruptedException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeaderShip();
        leaderElection.electLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from Zookeeper");
    }
}
