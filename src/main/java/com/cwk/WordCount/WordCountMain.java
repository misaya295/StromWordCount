package com.cwk.WordCount;

import clojure.lang.IFn;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountMain {



    public static void main(String[] args) {

        // 1、准备一个TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("WordCountSpout", new WordCountSpout(), 1);
        builder.setBolt("WordCountSplitBolt", new WordCountSplitBolt(), 2).shuffleGrouping("WordCountSpout");
        builder.setBolt("WordCountBolt", new WordCountBolt(), 4).fieldsGrouping("WordCountSplitBolt", new Fields("word"));



        //创建配置信息
        Config config = new Config();
//        config.setNumWorkers(2);



        //提交
        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordtopology", config, builder.createTopology());
        }

    }

}
