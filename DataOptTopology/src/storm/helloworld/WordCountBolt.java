package storm.helloworld;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/** 
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014年11月9日 下午10:09:43
 */


public class WordCountBolt extends BaseBasicBolt{
	private static final long serialVersionUID = 1L;
	Integer id;
	String name;
	Map<String, Integer> counters;
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		//this.collector = collector;
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}
	@Override
	public void execute(Tuple input,BasicOutputCollector collector) {
		String str = input.getString(0);
		
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		
		String send_str = null;
		int count = 0;
		
		for (String key : counters.keySet()) {
			if (count == 0) {
				send_str = "[" + key + " : " + counters.get(key) + "]";
			} else {
				send_str = send_str + ", [" + key + " : " + counters.get(key) + "]";
			}
			count++;	
		}
		send_str = "The count:" + count + " #### " + send_str; 
		collector.emit(new Values(send_str));	
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("send_str"));
	}
}
