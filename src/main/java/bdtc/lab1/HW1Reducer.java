package bdtc.lab1;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Редьюсер: считате среднее арифметичсекое значений метрик, полученных от {@link HW1Mapper},
 * выдаёт агрегированные значения метрик в выбранном диапазоне (секунды, минуты, часы)
 */
public class HW1Reducer extends Reducer<CompositeKey, IntWritable, Text, Text> {
    // vocabulary for metricId-metricName transformation in cache
    private Map<Integer, String> metricNames = new HashMap<>();

    /**
     * Предварительное чтение имен метрик из файла распределенного кэша
     */
    @Override
    public void setup(Context context) {
        try {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path filePath = new Path(cacheFiles[0]);
                parseJsonFile(filePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void parseJsonFile(Path filePath) throws IOException {
        InputStream is = new FileInputStream(filePath.toString());
        String jsonTxt = IOUtils.toString(is, StandardCharsets.UTF_8);
        JSONObject json = new JSONObject(jsonTxt);
        Iterator<String> intItr = json.keys();
        while (intItr.hasNext()) {
            String metricId = intItr.next();
            int metric = Integer.parseInt(metricId);
            metricNames.put(metric, (String) json.get(metricId));
        }
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int cnt = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            cnt += 1;
        }
        sum = sum / cnt;

        int metric_id = key.getMetric_id();
        long timestamp = key.getTimestamp();
        String metricName ="";
        if (metricNames.containsKey(metric_id)) {
            metricName = metricNames.get(metric_id);
            Configuration conf = context.getConfiguration();
            String time_range = conf.get("time_range");

            context.write(new Text(metricName + ", " + timestamp),
                    new Text(time_range + ", " + sum));
        }
        else {
            context.getCounter(CounterType.UNKNOWN_METRIC).increment(1);
        }
    }
}
