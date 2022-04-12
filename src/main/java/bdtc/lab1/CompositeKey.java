package bdtc.lab1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {
    private int metric_id;
    private long timestamp;

    // <init>
    public CompositeKey(int metric_id, long timestamp) {
        this.metric_id = metric_id;
        this.timestamp = timestamp;
    }

    // <init>
    public CompositeKey() {
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(metric_id);
        out.writeLong(timestamp);
    }
    public void readFields(DataInput in) throws IOException {
        metric_id = in.readInt();
        timestamp = in.readLong();
    }
    public int compareTo(CompositeKey pop) {
        int intcnt = Integer.compare(metric_id,pop.metric_id);
        return intcnt == 0 ? Long.compare(timestamp, pop.timestamp) : intcnt;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + metric_id;
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    public boolean equals(Object o){
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return this.metric_id == ((CompositeKey) o).metric_id && this.timestamp == ((CompositeKey) o).timestamp;
    }

    public String toString() {
        return metric_id + " ," + timestamp;
    }

    public int getMetric_id() {
        return metric_id;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
