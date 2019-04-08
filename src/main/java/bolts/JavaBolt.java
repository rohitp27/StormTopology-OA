package bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class JavaBolt extends BaseRichBolt{

	private OutputCollector _outputCollector;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_outputCollector = collector;
	}

	public void execute(Tuple input) {
		_outputCollector.emit(input, new Values(input.getMessageId()));
		_outputCollector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	

}
