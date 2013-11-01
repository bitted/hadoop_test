//package com.lakala.mrunit;
//
//import java.io.IOException;
//import java.util.List;
//
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mrunit.mapreduce.MapDriver;
//import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
//import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
//import org.apache.hadoop.mrunit.types.Pair;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//public class InvertedIndexJobTest {
//	MapDriver<LongWritable, Text, Text, Text> mapDriver;
//	ReduceDriver<Text, Text, Text, Text> reduceDriver;
//	MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mrDriver;
//
//	@Before
//	public void before() {
//		final InvertedIndexMapper myMapper = new InvertedIndexMapper();
//		final InvertedIndexReducer myReducer = new InvertedIndexReducer();
//
//		mapDriver = MapDriver.newMapDriver(myMapper);
//		reduceDriver = ReduceDriver.newReduceDriver(myReducer);
//		mrDriver = MapReduceDriver.newMapReduceDriver(myMapper, myReducer);
//	}
//
//	// 测试单个 关键词和网站名称
//	@Test
//	public void testMapperWithSingleKeyAndValueWithAssertion() throws IOException {
//		// k1
//		LongWritable k1 = new LongWritable(0);
//		// v1
//		Text v1 = new Text("传智播客,java培训");
//
//		mapDriver.withInput(k1, v1);
//		final List<Pair<Text, Text>> result = mapDriver.run();
//		System.out.println(result);
//		Assert.assertTrue(result.size() == 1);
//	}
//
//	// 测试单网站名称，多个关键词
//	@Test
//	public void testMapperWithSingleKeyAndMultiValues() throws IOException {
//		LongWritable k1 = new LongWritable(0);
//		Text v1 = new Text("传智播客,java培训,net培训,ios培训,php培训,网页培训,hadoop培训");
//
//		mapDriver.withInput(k1, v1);
//		final List<Pair<Text, Text>> result = mapDriver.run();
//		System.out.println(result);
//		Assert.assertTrue(result.size() == 6);
//	}
//
//	// 测试map、reduce联合测试
//	@Test
//	public void testReducer() throws Exception {
//		mrDriver.withInput(new LongWritable(0), new Text("传智播客,java培训,net培训,ios培训,php培训,网页培训,hadoop培训"));
//		mrDriver.withInput(new LongWritable(1), new Text("达内,java培训,net培训,ios培训,达内怎么了,达内"));
//
//		final List<Pair<Text, Text>> result = mrDriver.run();
//		System.out.println(result);
//		final Pair<Text, Text> javaTraining = new Pair<Text, Text>(new Text("java培训"), new Text("传智播客,达内"));
//		final Pair<Text, Text> tarenaHow = new Pair<Text, Text>(new Text("达内怎么了"), new Text("达内"));
//		Assert.assertTrue(result.contains(javaTraining));
//		Assert.assertTrue(result.contains(tarenaHow));
//	}
//}
//
//class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
//	// value=‘网站名称,关键词...’
//	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
//		if (value.getLength() == 0)
//			return;
//		String[] splited = value.toString().split(",");
//		for (int i = 1; i < splited.length; i++) {
//			// 关键词
//			final Text key2 = new Text(splited[i].trim());
//			// 网站名称
//			final Text value2 = new Text(splited[0].trim());
//			context.write(key2, value2);
//		}
//	};
//}
//
//class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
//	// key是关键词
//	// iterable是很多网站名称
//	protected void reduce(Text key, java.lang.Iterable<Text> iterable, Context context) throws java.io.IOException,
//			InterruptedException {
//		StringBuffer stringBuffer = new StringBuffer();
//		for (Text text : iterable) {
//			stringBuffer.append(text.toString()).append(",");
//		}
//		stringBuffer.setLength(stringBuffer.length() - 1);
//		// key是关键词
//		// value是所有网站名称的字符串
//		final Text value3 = new Text(stringBuffer.toString());
//		context.write(key, value3);
//	};
//}