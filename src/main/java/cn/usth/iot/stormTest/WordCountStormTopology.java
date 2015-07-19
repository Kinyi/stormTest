package cn.usth.iot.stormTest;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.messaging.local;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountStormTopology {

	public static class DataSourceSpout extends BaseRichSpout{
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;

		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}

		public void nextTuple() {
			//获取指定目录下的所有文件
			Collection<File> listFiles = FileUtils.listFiles(new File("E:\\StormMedia"), new String[]{"txt"}, true);
			for (File file : listFiles) {
				try {
					//解析每个文件中的每一行
					List<String> readLines = FileUtils.readLines(file);
					for (String line : readLines) {
						//把每一行发射出去
						this.collector.emit(new Values(line));
					}
					FileUtils.moveFile(file, new File(file.getAbsolutePath()+System.currentTimeMillis()));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("line"));
		}
		
	}
	
	public static class SplitBolt extends BaseRichBolt{
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;

		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}

		public void execute(Tuple input) {
			//获取tuple中发送过来的数据
			String line = input.getStringByField("line");
			//对每一行数据进行切割
			String[] words = line.split("\t");
			//把切割出来的单词发送到下一个bolt
			for (String word : words) {
				this.collector.emit(new Values(word));
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
		
	}
	
	public static class CountBolt extends BaseRichBolt{
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;

		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}

		Map<String, Integer> map = new HashMap<String, Integer>();
		public void execute(Tuple input) {
			//获取tuple中发送过来的数据
			String word = input.getStringByField("word");
			Integer value = map.get(word);
			if (value == null) {
				value = 0;
			}
			value++;
			//把数据保存到一个map对象中
			map.put(word, value);
			System.out.println("=============================");
			//把结果写出去
			for (Entry<String, Integer> entry : map.entrySet()) {
				System.out.println(entry);
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
		
	}
	
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("SPOUT", new DataSourceSpout());
		topologyBuilder.setBolt("SPLIT_BOLT", new SplitBolt()).shuffleGrouping("SPOUT");
		topologyBuilder.setBolt("COUNT_BOLT", new CountBolt()).shuffleGrouping("SPLIT_BOLT");
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology(WordCountStormTopology.class.getSimpleName(), new Config(), topologyBuilder.createTopology());
	}
	
}
