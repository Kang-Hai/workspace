package storm.helloworld;

import java.util.Map;

import storm.base.MacroDef;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
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

public class WordNormalizerBolt extends BaseBasicBolt  {
	
	private static final long serialVersionUID = -829840448328629270L;	
	@Override
	public void execute(Tuple input,BasicOutputCollector collector) {
		
		String sentence = input.getString(0);
		String[] words = sentence.split(MacroDef.FLAG_TABS);

		if (words.length != 0) {
			
			String domain = words[0];
			String[] do_word = domain.split("\\.");
			
			for (int i = 0; i < do_word.length; i++) {
				String word = do_word[i].trim();

				collector.emit(new Values(word));
			}
			String word = words[4].trim();		
			collector.emit(new Values(word));
			
			//collector.ack(input);
		}		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
