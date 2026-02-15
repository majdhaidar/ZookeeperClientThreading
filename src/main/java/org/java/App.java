package org.java;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Hello Zookeeper!");
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeaderShip();
        leaderElection.electLeader();
        leaderElection.watchTargetZnode();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from Zookeeper");
    }
}
