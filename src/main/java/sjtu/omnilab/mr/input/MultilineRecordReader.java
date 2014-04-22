package sjtu.omnilab.mr.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The record reader for MultilineInputFormat.
 * 
 * @author chenxm
 * 
 */
public class MultilineRecordReader extends RecordReader<LongWritable, TextArrayWritable> {
	private final static Logger logger = LoggerFactory.getLogger(MultilineRecordReader.class);
	
	private LineRecordReader reader;
	private LongWritable key;
	private TextArrayWritable value;
	private LongWritable lastKey = null;
	private Text lastValue = null;
	private static String sString = "";
	private static String eString = "";

	
	public MultilineRecordReader(String start, String end){
		if ( start.length() == 0 && end.length() == 0){
			logger.warn("Both start and end flags are empty.");
		}
		sString = start;
		eString = end;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		reader = new LineRecordReader();
		reader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		List<String> lines = new ArrayList<String>();
		if ( lastValue != null ){
			key = lastKey;
			lines.add(lastValue.toString() );
			lastKey = null; lastValue = null;
		}
		while ( reader.nextKeyValue() ){
			String line = reader.getCurrentValue().toString();
			SplitableLocation pair = MultilineInputFormat.checkSplitable(line, sString, eString);
			boolean added = false;
			if ( pair.lSplitable ){
				if ( lines.size() == 0 ){
					key = reader.getCurrentKey();
					lines.add(reader.getCurrentValue().toString());
					added = true;
				} else {
					value = new TextArrayWritable(convert(lines));
					lastKey = reader.getCurrentKey();
					lastValue = reader.getCurrentValue();
					return true;
				}
			}
			if ( pair.rSplitable ){
				if ( !added ){
					lines.add(reader.getCurrentValue().toString());
				}
				value = new TextArrayWritable(convert(lines));
				lastKey = null; lastValue = null;
				return true;
			}
			if ( ! added ){
				if ( lines.size() == 0 ){
					key = reader.getCurrentKey();
				}
				lines.add(reader.getCurrentValue().toString());
			}
		}
		if ( lines.size() > 0 ){
			value = new TextArrayWritable(convert(lines));
			return true;
		}
		return false;
	}

	private Text[] convert(List<String> s) {
		Text t[] = new Text[s.size()];
		for (int i = 0; i < t.length; i++) {
			t[i] = new Text(s.get(i));
		}
		return t;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public void close() throws IOException {
		if ( reader != null ){
			reader.close();
		}
	}
}
