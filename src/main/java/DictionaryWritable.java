import jdk.internal.util.xml.impl.Pair;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a custom Writable class which is used in job1
 * It basically implements all the methods which are necessary in order for the mapper and reducer in the hadoop library to be able to handle the custom Writable.
 * Those methods are marked by @OVERRIDE
 */
public class DictionaryWritable implements WritableComparable{
    private Text word;
    private Text docID;
    private Text category;
    private LongWritable word_cat_count;
    private LongWritable category_count;

    public LongWritable getCategory_count() {
        return category_count;
    }

    public void setCategory_count(LongWritable category_count) {
        this.category_count = category_count;
    }

    public DictionaryWritable(String word, String docID, String category, long word_cat_count,long count_) {
        this.word = new Text(word);
        this.docID = new Text(docID);
        this.category = new Text(category);
        this.word_cat_count = new LongWritable(word_cat_count);
        this.category_count = new LongWritable(count_);

    }

    public Text getWord() {
        return word;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    public void setDocID(Text docID) {
        this.docID = docID;
    }
    public Text getDocID() {
        return this.docID;
    }

    public Text getCategory() {
        return category;
    }

    public void setCategory(Text category) {
        this.category = category;
    }

    public LongWritable getWord_cat_count() {
        return word_cat_count;
    }

    public void setWord_cat_count(LongWritable word_cat_count) {
        this.word_cat_count = word_cat_count;
    }

    /**
     * increments the current word count for the category
     */
    public void inc_word_cat_count() {
        this.word_cat_count=new LongWritable(Long.parseLong(this.word_cat_count.toString())+1);
    }

    /**
     * increments the current category
     */
    public void inc_category_count() {
        this.category_count=new LongWritable(Long.parseLong(this.category_count.toString())+1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DictionaryWritable that = (DictionaryWritable) o;
        return word.equals(that.word) &&
                docID.equals(that.docID) &&
                category.equals(that.category) &&
                word_cat_count==that.word_cat_count&&
                that.category_count==category_count;

    }

    @Override
    public int hashCode() {
        return Objects.hash(word, docID, category, word_cat_count);
    }

    @Override
    public String toString() {
        String result = "Word:"+word.toString() + "," + "docID:"+this.docID.toString()+","+"category:"+this.category+","+"word_cat_count:"+this.word_cat_count+"category_count:"+this.category_count;
        return result;
    }

    @Override
    public int compareTo(Object o) {
        return this.equals(o)?0:1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.word.write(dataOutput);
        this.docID.write(dataOutput);
        this.category.write(dataOutput);
        this.word_cat_count.write(dataOutput);
        this.category_count.write(dataOutput);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word.readFields(dataInput);
        this.docID.readFields(dataInput);
        this.category.readFields(dataInput);
        this.word_cat_count.readFields(dataInput);
        this.category_count.readFields(dataInput);

    }

    public DictionaryWritable() {
        this.word=new Text();
        this.docID=new Text();
        this.category=new Text();
        this.word_cat_count=new LongWritable();
        this.category_count=new LongWritable();
    }

    public static DictionaryWritable read(DataInput in) throws IOException {
        DictionaryWritable w = new DictionaryWritable();
        w.readFields(in);
        return w;
    }
}
