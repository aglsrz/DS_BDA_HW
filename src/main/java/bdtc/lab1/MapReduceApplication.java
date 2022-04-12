package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Arrays;
import java.util.List;


@Log4j
public class MapReduceApplication {

    private final static List<String> types = Arrays.asList("min", "sec", "hour");

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            throw new RuntimeException("You should specify input, output folders and time range");
        }

        if (!types.contains(args[2])){
            log.error("Invalid time range argument");
            return;
        }

        // задаём диапазон времени для агрегации
        Configuration conf = new Configuration();
        conf.set("time_range", args[2]);

        Job job = Job.getInstance(conf, "metrics aggregator");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //добавление файла в кэш
        try {
            job.addCacheFile(new Path("metrics.json").toUri());
        }
        catch (Exception e){
            System.out.println("File Not Added");
            System.exit(1);
        }

        // задаем выходной формат sequence file
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        Counter counter = job.getCounters().findCounter(CounterType.UNKNOWN_METRIC);
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
    }
}
