package storm.helloworld;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import storm.base.MacroDef;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.topology.base.BaseRichSpout;
/** 
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014年11月9日 上午11:26:29
 */


public class ReadFileSpout extends BaseRichSpout {
	private static final long serialVersionUID = 3142804203962362581L;
	private SpoutOutputCollector collector;
	FileInputStream fis;
	InputStreamReader isr;
	BufferedReader br;
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {

		this.collector = collector;
		
		String file = (String)conf.get("INPUT_PATH");

		try {

			this.fis = new FileInputStream(file);
			this.isr = new InputStreamReader(fis, MacroDef.ENCODING);
			this.br = new BufferedReader(isr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Override
	public void nextTuple() {
		String str = "";
		try {
			while ((str = this.br.readLine()) != null) {
				this.collector.emit(new Values(str));
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("str"));
	}
}
