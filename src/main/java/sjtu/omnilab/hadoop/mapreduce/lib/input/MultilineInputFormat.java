package sjtu.omnilab.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Define an Input Format that each data block is indicated by a start string or
 * an end string, or both. This format is compatible with the TextInputFormat
 * when start/end strings are absent and the file is splitable around each line.
 * 
 * @author chenxm
 * 
 */
public class MultilineInputFormat extends FileInputFormat<LongWritable, TextArrayWritable> {
	private final static Logger logger = LoggerFactory.getLogger(MultilineInputFormat.class);
	
	private static final String START_STRING = "mapreduce.input.multilineinputformat.startstr";
	private static final String END_STRING = "mapreduce.input.multilineinputformat.endstr";
	private static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
	
	private String startString = "";
	private String endString = "";
	private static final double SPLIT_SLOP = 1.1; // 10% slop

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return codec == null;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) 
									throws IOException {
		// BUG NOTE: this function is not called when a small test data is involved.
		// So the start and end string modifier would not work here.
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus status : files) {
			splits.addAll(getSplitsForFile(status, job));
		}
		// Save the number of input files in the job-conf
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		logger.info("Total # of splits: " + splits.size());
		return splits;
	}
	
	@Override
	public RecordReader<LongWritable, TextArrayWritable> createRecordReader(
			InputSplit genericSplit, TaskAttemptContext context)
									throws IOException {
		context.setStatus(genericSplit.toString());
		this.startString = getMultilineStartString(context);
		this.endString = getMultilineEndString(context);
		logger.info("Block start string: " + this.startString);
		logger.info("Block end string: " + this.endString);
		return new MultilineRecordReader(this.startString, this.endString);
	}

	/**
	 * Get a list of Input Splits for specific file in DFS.
	 * 
	 * @param file The file to process
	 * @param job Global job configuration
	 * @return A list of InputSplit instances.
	 * @throws IOException
	 */
	public List<InputSplit> getSplitsForFile(FileStatus file, JobContext job) 
									throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Path fileName = file.getPath();
		if (file.isDir()) {
			throw new IOException("Not a file: " + fileName);
		}
		FileSystem fs = fileName.getFileSystem(job.getConfiguration());

		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);
		long length = file.getLen();
		BlockLocation[] blkLocations = fs
				.getFileBlockLocations(file, 0, length);

		// generate splits
		if ((length != 0) && isSplitable(job, fileName)) {
			LineReader lr = null;
			try {
				FSDataInputStream in = fs.open(fileName);
				lr = new LineReader(in, job.getConfiguration());
				Text line = new Text();
				int numLines = 0;
				long begin = 0;
				long blkLength = 0;
				int num = -1;
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(blockSize, minSize, maxSize);
				while ((num = lr.readLine(line)) > 0) {
					double splitSlop = (double) blkLength / splitSize;
					if (splitSlop > 1 && splitSlop < SPLIT_SLOP) {
						SplitableLocation pair = checkSplitable(line.toString(), startString, endString);
						if (pair.lSplitable) {
							int blkIndex = getBlockIndex(blkLocations, begin);
							splits.add(createFileSplit(fileName, begin,
									blkLength, blkLocations[blkIndex].getHosts()));
							begin += blkLength; blkLength = 0; numLines = 0;
						} else if (pair.rSplitable) {
							numLines++; blkLength += num;
							int blkIndex = getBlockIndex(blkLocations, begin);
							splits.add(createFileSplit(fileName, begin,
									blkLength, blkLocations[blkIndex].getHosts()));
							begin += blkLength; blkLength = 0; numLines = 0;
						}
					}
					numLines++; blkLength += num;
				}
				if (numLines != 0) {
					int blkIndex = getBlockIndex(blkLocations, begin);
					splits.add(createFileSplit(fileName, begin, blkLength,
							blkLocations[blkIndex].getHosts()));
				}
			} finally {
				if (lr != null)
					lr.close();
			}
		} else if (length != 0) {
			splits.add(new FileSplit(fileName, 0, length, blkLocations[0]
					.getHosts()));
		} else {
			// Create empty hosts array for zero length files
			splits.add(new FileSplit(fileName, 0, length, new String[0]));
		}
		return splits;
	}
	
	/**
	 * Decide whether this line is splitable.
	 * @param line
	 * @param startStr
	 * @param endStr
	 * @return
	 */
	public static SplitableLocation checkSplitable(String line, String startStr, String endStr){
		SplitableLocation pair = new SplitableLocation();
		if (startStr.length() == 0 && endStr.length() == 0) {
			// Deprecated version that each line is splitable.
			pair.lSplitable = true; 
			pair.rSplitable = true;
		} else {
			// Explicit expression that a line is left-splitable iif.
			// startString is given and the line starts with the given string.
			// Similar situation for the right-splitable.
			line = line.trim();
			if (startStr.length() > 0)
				pair.lSplitable = line.startsWith(startStr);
			if (endStr.length() > 0)
				pair.rSplitable = line.startsWith(endStr);
		}
		return pair;
	}

	/**
	 * Create a FileSplit from a file in DFS.
	 * 
	 * @param fileName The file path in DFS
	 * @param begin The position of the first byte in the file to process
	 * @param length The number of bytes in the file to process
	 * @param hosts The list of hosts containing the block, possibly null
	 * @return A FileSplit instance
	 */
	protected static FileSplit createFileSplit(Path fileName, long begin,
			long length, String[] hosts) {
		return (begin == 0) ? new FileSplit(fileName, begin, length - 1, hosts)
				: new FileSplit(fileName, begin - 1, length, hosts);
	}

	/**
	 * Set the start string for this Input Format.
	 * 
	 * @param job
	 * @param startstr
	 */
	public static void setMultilineStartString(Job job, String startstr) {
		job.getConfiguration().set(START_STRING, startstr);
	}

	/**
	 * Get the start string for this Input Format.
	 * 
	 * @param job
	 * @return
	 */
	public static String getMultilineStartString(JobContext job) {
		return job.getConfiguration().get(START_STRING, "");
	}

	/**
	 * Set the end string for this Input Format.
	 * 
	 * @param job
	 * @param endstr
	 */
	public static void setMultilineEndString(Job job, String endstr) {
		job.getConfiguration().set(END_STRING, endstr);
	}

	/**
	 * Get the end string for this Input Format.
	 * 
	 * @param job
	 * @return
	 */
	public static String getMultilineEndString(JobContext job) {
		return job.getConfiguration().get(END_STRING, "");
	}
}
