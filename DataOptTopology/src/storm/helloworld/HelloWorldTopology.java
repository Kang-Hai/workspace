package storm.helloworld;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author blogchong
 * @Blog www.blogchong.com
 * @email blogchong@gmail.com
 * @QQ_G 191321336
 * @version 2014年11月9日 下午21:50:29
 */


// BoltDeclarer backtype.storm.topology.InputDeclarer.fieldsGrouping(String
// componentId, Fields fields)

// 这是一个简单实例，统计词频wordcoutn。storm中的helloworld

public class HelloWorldTopology {

	private static TopologyBuilder builder = new TopologyBuilder();

	public static void main(String[] args) throws InterruptedException,AlreadyAliveException, InvalidTopologyException {
		
			Config config = new Config();
			builder.setSpout("Random", new ReadFileSpout(), 1);
			builder.setBolt("Norm", new WordNormalizerBolt(), 2).shuffleGrouping("Random");
			builder.setBolt("Count", new WordCountBolt(), 2).fieldsGrouping("Norm", new Fields("word"));
			builder.setBolt("print", new PrintWorldCountBolt(), 2).shuffleGrouping("Count");		
		
		if (args != null && args.length > 0) {
			config.setDebug(false);
			config.setNumWorkers(2);
			config.put("INPUT_PATH", args[0]);
			StormSubmitter.submitTopology("wordcount", config,builder.createTopology());		
		} 
		else {
			//config.setDebug(true);
			config.setMaxTaskParallelism(1);
			config.put("INPUT_PATH", "c:\\domain.log");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("helloword", config,builder.createTopology());
		}
	}
}
