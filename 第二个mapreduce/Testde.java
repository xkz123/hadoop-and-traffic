package cn.itcast.hadoop.mr.em;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import cn.itcast.hadoop.mr.em.Test.intpa;

public class Testde {
	
	 public static class intpa2 implements WritableComparable<intpa2>{
		   String first;
		   String second;
		   String  count;
		   
		   
		
		public int compareTo(intpa2 o) {
			// TODO Auto-generated method stub
			intpa2 itp=(intpa2)o;
		
			if(!second.equals(itp.getSecond()) )
			{
				
				return second.compareTo(itp.getSecond());
				
			}
			else
			{
				
				
				return Integer.parseInt(this.first)>Integer.parseInt(itp.getFirst())?1:-1;
			}
			
		}
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(first);;
			out.writeUTF(second);
			out.writeUTF(count);
			
		}
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.first= in.readUTF();
			this.second=in.readUTF();
			this.count=in.readUTF();
		}
		public void set(String first,String second,String count){
			this.first=first;
			this.second=second;
			this.count=count;
			
		}
		
		public String getFirst() {
			return first;
		}
		public void setFirst(String first) {
			this.first = first;
		}
		public String getSecond() {
			return second;
		}
		public void setSecond(String second) {
			this.second = second;
		}
		public String getCount() {
			return count;
		}
		public void setCount(String count) {
			this.count = count;
		}
		 
	 }
	
	 public static class TrafficMapper extends Mapper<LongWritable, Text, intpa2, NullWritable>
	 {

		 private intpa2 itp=new intpa2();
		 private int a=0;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String [] lines=value.toString().split("\t");
			itp.set((a++)+"", lines[1], lines[2]+" "+lines[0]);
			
			context.write(itp, NullWritable.get());
			
			
		}
		 
		 
	 }

	 public static class TrafficReducer extends Reducer<intpa2, NullWritable,Text, NullWritable>{

		 private Text k=new Text();
		@Override
		protected void reduce(intpa2 itp, Iterable<NullWritable> v2s,Context context)
				throws IOException, InterruptedException {
			String str= itp.getFirst()+" "+itp.getSecond()+" "+itp.getCount();
			k.set(str);
			context.write(k, NullWritable.get());
		}
		 
		 
	 }
	 
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Testde.class);
		
		job.setMapperClass(TrafficMapper.class);
		job.setMapOutputKeyClass(intpa2.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//job.setCombinerClass(TrafficReducer.class);
		
		job.setReducerClass(TrafficReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
	
		//job.setCombinerClass(TrafficReducer.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);
		
	}

}
