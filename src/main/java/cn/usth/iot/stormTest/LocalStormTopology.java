package cn.usth.iot.stormTest;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
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

public class LocalStormTopology {
	
	public static class DataSourceSpout extends BaseRichSpout{
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;

		/**
		 * 在本实例运行时只被调用一次
		 */
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}

		/**
		 * 死循环调用 心跳
		 */
		int i = 0;
		public void nextTuple() {
			System.out.println("i : " + i);
			//
			this.collector.emit(new Values(i++));
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * 声明字段名称
		 */
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			//
			declarer.declare(new Fields("num"));
		}
	}
	
	public static class SumBolt extends BaseRichBolt{
		private Map stormConf; 
		private TopologyContext context;
		private OutputCollector collector;

		//只会被调用一次
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}

		//死循环，循环的获取上一级发送过来的数据(spout/bolt)
		int sum = 0;
		public void execute(Tuple input) {
			Integer num = input.getIntegerByField("num");
			sum += num;
			System.out.println("sum is : " + sum);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}
	}
	
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("SPOUT_ID", new DataSourceSpout());
		topologyBuilder.setBolt("BOLT_ID", new SumBolt()).shuffleGrouping("SPOUT_ID");
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology(LocalStormTopology.class.getSimpleName(), new Config(), topologyBuilder.createTopology());
	}

}
