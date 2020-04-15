import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
public class FrequencyCategory implements WritableComparable  {
    private Text category;
    private LongWritable frequency;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FrequencyCategory that = (FrequencyCategory) o;
        return category.equals(that.category) &&
                frequency.equals(that.frequency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, frequency);
    }

    public FrequencyCategory(Text category) {
        this.category = category;
    }

    public FrequencyCategory(Text category, LongWritable frequency) {
        this.category = category;
        this.frequency = frequency;
    }

    public Text getCategory() {
        return category;
    }

    public void setCategory(Text category) {
        this.category = category;
    }

    public LongWritable getFrequency() {
        return frequency;
    }

    public void setFrequency(LongWritable frequency) {
        this.frequency = frequency;
    }

    @Override
    public int compareTo(Object o) {
        if(o.getClass()!=this.getClass())return -1;
        FrequencyCategory f=(FrequencyCategory)o;
        return this.equals(o)?0:this.getCategory().equals(f.getCategory())?this.getFrequency().compareTo(f.getFrequency()):-1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.category.write(dataOutput);
        this.frequency.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.category.readFields(dataInput);
        this.frequency.readFields(dataInput);
    }
}
