package cn.itcast.hadoop.mr.em;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//ddimport cn.itcast.hadoop.mr.em.Test.intpa;

public class Test3{
	
   public static class intpa implements WritableComparable<intpa>{
	   String first;
	 
	 
	   public void set(String first)
	   {
		   this.first=first;
		   
		   
	   }
	   
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(first);
	
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.first= in.readUTF();
	
	}
	public int compareTo(intpa o) {
		// TODO Auto-generated method stub
	    int v1= Integer.parseInt(this.first);
	    int v2=Integer.parseInt(((intpa)o).first);
	    return v1<v2?-1:1;
	}
	public String getFirst() {
		return first;
	}
	public void setFirst(String first) {
		this.first = first;
	}
	   
   }
   
   public static class TrafficMapper extends Mapper<LongWritable, Text, intpa, Text>{


		//Text k=new Text();
		private int j=0;
		private intpa itp=new intpa();
		private int sum=0;
		private int count=0;
		//private StringBuilder sb=new StringBuilder();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		 String line = value.toString();
		 String[] lines = line.split(",");
	
		sum+= Integer.parseInt(lines[2]);
		 count++;
		 if((count-1)%6==0&&count!=1){
				if(count==7)
				{
					//sb.append(sum/7);
					//sb.append(" ");
					itp.set((j++)+"");
					context.write(itp, new Text((sum/7)+""));
					sum=0;
				}
			
				else 
				{
				//sb.append(sum/6);
				//sb.append(" ");
					itp.set((j++)+"");
				context.write(itp, new Text((sum/6)+""));
				sum=0;
				}
			
			}
			if(count==288)
			{
				itp.set((j++)+"");
				context.write(itp, new Text((sum/5)+""));
			}
		
	}
			

	}
	   
	   
   public static class TrafficReducer extends Reducer<intpa, Text, Text, Text>{

	   private int j=0;
	@Override
	protected void reduce(intpa key, Iterable<Text> data,Context context)
			throws IOException, InterruptedException {
	j++;
		for(Text i:data){
		context.write(new Text(j+""), i);
		}
	}
	   
	   
   }


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Traffic.class);
		
		job.setMapperClass(TrafficMapper.class);
		job.setMapOutputKeyClass(intpa.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(TrafficReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		//job.setCombinerClass(TrafficReducer.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);
		
		
		
		
	}

}
