import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import bolts.JavaBolt;

public class TopologyDriver {
	public static void main(String[] args) {
		
		//TODO Read from config
		//Spouts created here
		BrokerHosts zookeeperHosts = new ZkHosts("zookeeper1:2181,zookeeper2:2181");
		String topicName = "onlineDataStream";
		String spoutId = "onlineDataStreamSpout";
		
		SpoutConfig spoutConfig = new SpoutConfig(zookeeperHosts, topicName, "/" + topicName, spoutId);
		spoutConfig.startOffsetTime = System.currentTimeMillis();
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		//creating topology
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("kafkaSpout", kafkaSpout);
		topologyBuilder.setBolt("javaBolt", new JavaBolt()).shuffleGrouping("kafkaSpout");
		
		/*
		 * Working with local cluster
		 */
		/*
		Config config = new Config();
		config.setDebug(true);
		config.setNumWorkers(2);
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("localTopology", config, topologyBuilder.createTopology());
		
		
		Utils.sleep(15000);
		localCluster.killTopology("localTopology");
		localCluster.shutdown();
		*/
		
		/*
		 * Working with actual cluster
		 */
		Config remoteClusterTopologyConfig = new Config();
		remoteClusterTopologyConfig.setNumWorkers(4);
		remoteClusterTopologyConfig.setMessageTimeoutSecs(20);
		
		try {
			StormSubmitter.submitTopology("onlineAnalyticsTopology", 
					remoteClusterTopologyConfig, 
					topologyBuilder.createTopology());
		}catch (AlreadyAliveException e) {
			e.printStackTrace();
		}catch (InvalidTopologyException e) {
			e.printStackTrace();
		}catch (AuthorizationException e) {
			e.printStackTrace();
		}
	}
}









