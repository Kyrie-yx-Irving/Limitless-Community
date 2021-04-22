package yx.unique.zookeeper.leaderselect.unfair;


import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FairLeaderSelect {

    private static ZooKeeper zk;
    private static final int SESSION_TIME_OUT = 3000;
    private static String nodeVal;

    public static void main(String[] args) throws Exception {
        zk = new ZooKeeper("127.0.0.1:2181", SESSION_TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                log.info("{} - {} - {}", event.getType(), event.getPath(), event.getState());
            }
        });
        String leaderPath = "/server/leader";
        nodeVal = zk.create(leaderPath, getLocalIp().getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        selection();
        Scanner in = new Scanner(System.in);
        log.info(in.nextLine());
        zk.close();
    }


    private static void selection() throws Exception {
        List<String> children = zk.getChildren("/server", null);
        Collections.sort(children);
        String formerNode = "";
        for (int i = 0; i < children.size(); i++) {
            String node = children.get(i);
            if (nodeVal.equals("/server/" + node)) {
                if (i == 0) {
                    // 第一个
                    log.info("我被选为leader节点了");
                } else {
                    formerNode = children.get(i - 1);
                }
            }
        }
        if (!"".equals(formerNode)) {
            log.info("我竞选失败了");
            zk.getData("/server/" + formerNode, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    log.info("{} - {} - {}", event.getType(), event.getPath(), event.getState());
                    try {
                        if (Event.EventType.NodeDeleted.equals(event.getType())) {
                            selection();
                        }
                    } catch (Exception e) {
                        // ignore the exception
                    }
                }
            }, null);
        }
    }


    private static String getLocalIp() {
        return "127.0.0.1";
    }
}
