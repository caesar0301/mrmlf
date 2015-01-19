package cn.edu.sjtu.omnilab.mrmlf;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class MultilineInputFormatSample extends Configured implements Tool {
	
	public static class MultilineMapper extends Mapper<LongWritable, TextArrayWritable, LongWritable, TextArrayWritable> {
		@Override
		protected void map(LongWritable key, TextArrayWritable value,Context context) 
										throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class MultilineReducer extends Reducer<LongWritable, TextArrayWritable, Text, NullWritable> {
		@Override
		public void reduce(LongWritable key, Iterable<TextArrayWritable> values, Context context)
										throws IOException, InterruptedException {
			for (TextArrayWritable val : values) {
				String line = "";
				for ( Writable text : val.get()){
					line = text.toString().replaceAll("\\s", ",");
				}
				context.write(new Text(line), NullWritable.get());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "TestMultilineInputFormat");
		job.setJarByClass(MultilineInputFormatSample.class);
		job.setMapperClass(MultilineMapper.class);
		job.setReducerClass(MultilineReducer.class);
		job.setInputFormatClass(MultilineInputFormat.class);
		MultilineInputFormat.setMultilineStartString(job, "<CD>");
		MultilineInputFormat.setMultilineEndString(job, "</CD>");

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(TextArrayWritable.class);

		job.setOutputKeyClass(TextArrayWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MultilineInputFormatSample(), args);
		System.exit(res);
	}
}
