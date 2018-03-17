import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threadhold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threashold
			if(count < threashold){
				return;
			}
			//this is --> cool = 20
			StringBuilder stringBuilder = new StringBuilder();
			for(int i = 0; i < words.length - 1; i++){
				stringBuilder.append(words[i]).append(" ");
			}
			//what is the outputkey?
			//what is the outputvalue?
			String outputKey = stringBuilder.toString().trim();
			String outputValue = words[words.length - 1]; //实际上后面还要跟count,在下面实现
			
			//write key-value to reducer?
			if(!((outputKey == null) || (outputKey.length() < 1))){
				context.write(new Text(outputKey), new Text(outputValue + "=" + count));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
			//1. save in map
			HashMap<Integer, List<String>> map = new HashMap<Integer, List<String>>();
			for(Text val: values){
				String curValue = val.toString().trim();
				String word = curValue.split("=")[0].trim();
				int count = Integer.parseInt(curValue.split("regex")[1].trim());
				if(map.containsKey(count)){
					map.get(count).add(word);
				}
				else{
					List<String> list = new ArrayList<String>();
					list.add(word);
					map.put(count, list);
				}
			}

			//2. implement maxHeap
			PriorityQueue<HashMap.Entry<Integer, List<String>>> pq = new PriorityQueue<>(1, new Comparator<HashMap.Entry<Integer, List<String>>>(){
				public int compare(HashMap.Entry<Integer, List<String>> a, HashMap.Entry<Integer, List<String>> b){
					return b.getKey() - a.getKey();
				}
			});

			//3. put into heap
			for(HashMap.Entry<Integer, List<String>> entry : map.entrySet()){
				pq.offer(entry);
			}

			//generate result
			for(int i = 0; i < n && i < pq.size(); i++){
				int count = pq.peek().getKey();
				List<String> words = pq.poll().getValue();
				for(String word : words){
					context.write(new DBOutputWritable(key.toString(), word, count), NullWritable.get());
				}
			}
			}
		}
	}
}
