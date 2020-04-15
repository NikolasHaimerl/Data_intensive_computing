import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.util.*;
import java.io.File;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Qui_Square {
    /**
     * is a counter for all the reviews in the dataset
     */
    public enum DOCUMENT_COUNTER {
        TOTAL_NUMBER_DOCUMENTS
    }

    public static class OrderMergeMapper extends Mapper<Object, Text, Text, MapWritable> {
        /**
         *
         * @param key is the hashcode of the output of job2
         * @param value is the string representation of the word:category-quisquarevalue mapping. Has to be read in for further use
         *             This mapper basically reverses the order at which the previous output (job2) was outputted. So every category gets a map of words with their corresponding Qui-Square value
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] k_v = value.toString().split("\t");
            Text word = new Text(k_v[0]);
            Map<Text, Double> catQuis = getCategoryQuiSquareMap(k_v[1]);
            Set keys = catQuis.keySet();

            for (Object it : keys) {
                MapWritable wordQuisValue = new MapWritable();
                String catkey = it.toString();
                Object quivalue = catQuis.get(it);
                double v = Double.parseDouble(quivalue.toString());
                wordQuisValue.put(word, new DoubleWritable(v));
                context.write(new Text(catkey), wordQuisValue);
            }

        }

        /**
         *
         * @param str is the mapping of job2
         * @return is the mapping of category:qui-square value
         */
        public Map<Text, Double> getCategoryQuiSquareMap(String str) {
            Map<Text, Double> returnMap = new HashMap<>();
            StringTokenizer itr = new StringTokenizer(str);
            while (itr.hasMoreTokens()) {
                String next = itr.nextToken();
                String cat = next.substring(next.indexOf("{") + 1, next.indexOf(":"));
                String quis = next.substring(next.indexOf(":") + 1, next.indexOf("}"));
                returnMap.put(new Text(cat), (Double.parseDouble(quis)));
            }

            return returnMap;
        }
    }

    public static class OrderMergeReducer extends Reducer<Text, MapWritable, Text, Text> {
        /**
         *
         * @param key is the category which is to be mapped to the correpsonding words and their qui-square values
         * @param values all the word:qui-square value pairs where the word appears in this categoiry
         *               This reducer maps the final ouput to the form: category: word1:value1 word2:value2...
         */
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            List<Pair<String, Double>> pairs = new ArrayList<>();
            for (MapWritable itr : values) {
                Set keys = itr.keySet();
                for (Object o : keys) {
                    double s = Double.parseDouble(itr.get(o).toString());
                    pairs.add(new Pair<String, Double>(o.toString(), s));
                }
            }
            Collections.sort(pairs, Comparator.comparing(p -> -p.getSecond()));
            String out = "";
            for (int i = 0; i < 200; i++) {
                Pair<String, Double> pair = pairs.get(i);
                out += pair.getFirst() + ":" + pair.getSecond() + "\t";
            }
            context.write(key, new Text(out));
        }
    }

    //**************************************************************************************************************Job3**********************************************************************************************//
    public static class CategoryWordMapper extends Mapper<Object, Text, Text, MapWritable> {
        /**
         *
         * @param key is a hashcode of the output of job1
         * @param value is the output of jub1 in a Text representation. It contains key and value. For further processing only the value is interessting since it contains all the necessary information
         * This mapper reads in the output of the previous job and stores it in a MapWritable for further use. This time the key is the word so one can iterate over all word-category combinations in the reducer
         *              and therefore extract the total number of how often a word comes up in the dataset (Only one occurence per category/review is counted).
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String val = value.toString();
            StringTokenizer pieces = new StringTokenizer(val, "\t}{");
            pieces.nextToken();
            MapWritable result = new MapWritable();
            while (pieces.hasMoreTokens()) {
                //the string reprecentation from the earlier output is {category:"+cat+"\t"+"count_word_in_category:"+count_word_in_category+"\t"+"word:"+ke+"\t"+"total_count_category:"+total_count_category+"}
                //first value is always the description and the second is the value of a dictionary entry
                String[] catval = pieces.nextToken().split(":");
                result.put(new Text(catval[0]), new Text(catval[1]));
            }
            Text word = (Text) result.get(new Text("word"));
            context.write(word, result);
        }
    }


    public static class ReduceCategoryWord extends Reducer<Text, MapWritable, Text, Text> {
        //acts as a placeholder for later use
        private static Long documentnumer;
        /**
         *
         * @param key is a single word occuring in the review (only once per docID)
         * @param values contains for one word: a category it occured in, the number of occurences of the word in that category, the number of occurences of the category in all documents
         *  The reducer is responsible for calculating the QuiSquare value. Word_count calculates the occurence of the word across all categories (only one occurence per docID)
         *               Due to the nature of the formula for calculating the QuiSquare value only word: category1:value1 category1:value2... format can be extracted
         */
        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            /**
             * result maps all the categories to categories to the quiSquare Value of the momentary word in use (key)
             */
            MapWritable result = new MapWritable();
            Map<String, Double> frequency_cat = new HashMap<String, Double>();
            Map<String, Double> frequency_word = new HashMap<String, Double>();
            double A, B, C, D;
            double word_count = 0;
            for (MapWritable mappings : values) {
                String category = mappings.get(new Text("category")).toString();
                double count_word_in_category = Integer.parseInt(mappings.get(new Text("count_word_in_category")).toString());
                double total_count_category = Integer.parseInt(mappings.get(new Text("total_count_category")).toString());
                frequency_word.put(category, count_word_in_category);
                frequency_cat.put(category, total_count_category);
                word_count += count_word_in_category;
            }

            Set<String> categories = frequency_word.keySet();
            for (String cat : categories) {
                Text cattext = new Text(cat);
                double frequence_word = frequency_word.get(cat);
                double frequence_cat = frequency_cat.get(cat);
                double N = documentnumer;
                A = frequence_word;
                B = word_count - A;
                C = frequence_cat - A;
                D = N - A - B - C;
                // Forumala for calcualting the QuiSquare Value
                double quisquare = Math.pow(A * D - B * C, 2) / ((A + B) * (A + C) * (B + D) * (C + D)) * N;
                result.put(cattext, new DoubleWritable(quisquare));
            }
            Set keys = result.keySet();
            String out = "";
            for (Object k : keys) {
                String qui = result.get(k).toString();
                String cat = k.toString();
                //Builds the string representation of the output
                out += "{" + cat + ":" + qui + "} ";
            }
            out = out.substring(0, out.length() - 1);
            context.write(key, new Text(out));
        }

        /**
         *The setup is responsible for reading in the document number which is necessary for the calculation of the Qui-Square value
         */
        public void setup(Context context) throws FileNotFoundException, InterruptedException {
            this.documentnumer=context.getConfiguration().getLong(DOCUMENT_COUNTER.TOTAL_NUMBER_DOCUMENTS.name(),0);

        }
    }

    //**************************************************************************************************************Job2**********************************************************************************************//

    /**
     * This method is used to extract the reviewText one json line entry.
     *
     * @param jsonLine one JsonLine containing all the information of one review
     * @return a String containing the category
     */
    public static String extractReviewText(String jsonLine) {
        String result = jsonLine.substring(jsonLine.indexOf("\"reviewText\": \"") + 15, jsonLine.indexOf(", \"overall\":") - 1);
        return result;
    }

    /**
     * This method is used to extract the category of one json line entry.
     * @param jsonLine one JsonLine containing all the information of one review
     * @return a String containing the category
     */
    public static String extractCategoryText(String jsonLine) {
        String result = jsonLine.substring(jsonLine.indexOf("\"category\": \"") + 13, jsonLine.length() - 2);
        return result;
    }

    /**
     *TOTAL_DOCUMENTS acts as a counter for all the documents in the review set
     *  **/
    public enum Count {
        TOTAL_DOCUMENTS
    }

    /**
     * Mapper for the first job.
     * It takes in every review of the amazon review dataset and maps the categories to a Dictionary
     *  **/
    public static Map<Text, IntWritable> categoryFrequency = new HashMap<Text, IntWritable>();
    public static class CategoryMapper extends Mapper<Object, Text, Text, DictionaryWritable> {
        /**
         * reads in all the stopwords from the config file and stores it for later use
         */
        private Set<String> stopWords = new HashSet<String>();
        protected void setstopwords(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            for (String stopword : conf.get("stop.words").split(",")) {
                stopWords.add(stopword);
            }
        }

        /**
         *
         * @param o is a hashcode of the review and acts as an ID
         * @param values are the actual contents of the review
         * The mapper extracts the category of the review and the review text. Then it splits the words of the review text and stores them individually
         * If the word is contained in the stopwords from earlier it is not stores.This is also the case if the word is a single character
         * The dictionary contains the category, the word, the word count and the category count for later use
         */
        public void map(Object o, Text values, Context context) throws IOException, InterruptedException {
            String category = extractCategoryText(values.toString()).toLowerCase();
            String text=extractReviewText(values.toString());
            StringTokenizer itr = new StringTokenizer(text.toLowerCase(), " .!?,;:()[]{}-_\"`~#&*%$\\/ \n\t0123456789");
            Set<String> words = new HashSet<String>();
            if (stopWords.isEmpty()) {
                setstopwords(context);
            }
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();

                if (stopWords.contains(token) || token.length() <= 1 || words.contains(token)) {
                    continue;
                }
                words.add(token);
            }
            for(String word : words){
                DictionaryWritable result=new DictionaryWritable(word,o.toString(),category,1L,1L);
                context.write(new Text(category),result);
            }

            context.getCounter(DOCUMENT_COUNTER.TOTAL_NUMBER_DOCUMENTS).increment(1);

        }

    }


    public static class CategoryReducer extends Reducer<Text, DictionaryWritable, Text, Text> {
        /**
         *
         * @param key is the category which was mapped earlier
         * @param values are all the dictionaries which correspond to the key
         * The reducer counts for the category (key) all the words which appear in the entire amazon review set and stores the counter in a map.
         * The occurences is a counter to set the number of documents the category (key) appears in
         */
        public void reduce(Text key, Iterable<DictionaryWritable> values, Context context) throws IOException, InterruptedException {
            int occurences=0;
            Map<String,Long> words=new HashMap<>();
            Set<String> docIDs=new HashSet<>();
            for(DictionaryWritable map : values){
                String id=map.getDocID().toString();
                String word=map.getWord().toString();
                if(words.containsKey(word)){
                    Long i=words.get(word);
                    words.replace(word,i+1);
                }
                else{
                    words.put(word,1L);
                }
                //in every review there is only one category but multiple category-word combinations. So per document the counter for the categories is only triggered once
                if(docIDs.contains(id)){
                    continue;
                }
                else{
                    docIDs.add(id);
                    occurences++;
                }
            }
            Set<String> keys=words.keySet();
            for(String ke : keys){
                Long count_word_in_category=words.get(ke);
                String cat=key.toString();
                String total_count_category= Long.toString(occurences);
                String res="{category:"+cat+"\t"+"count_word_in_category:"+count_word_in_category+"\t"+"word:"+ke+"\t"+"total_count_category:"+total_count_category+"}";
                context.write(key,new Text(res));
            }
        }
    }
    //**************************************************************************************************************Job1**********************************************************************************************//


    public static void main(String[] args) throws Exception {
        //Initialize the config file
        Configuration config = new Configuration();
        //Read all the stopwords into the config file and give the key *stop.word* to it
        config.set("stop.words", "a's,able,about,above,according,accordingly,across,actually,after,afterwards,again,against,ain't,all,allow,allows,almost,alone," +
                "along,already,also,although,always,am,among,amongst,an,and,another,any,anybody,anyhow,anyone,anything,anyway,anyways,anywhere,apart,appear," +
                "appreciate,appropriate,are,aren't,around,as,aside,ask,asking,associated,at,available,away,awfully,be,became,because,become,becomes,becoming," +
                "been,before,beforehand,behind,being,believe,below,beside,besides,best,better,between,beyond,both,brief,but,by,c'mon,c's,came,can,can't,cannot," +
                "cant,cause,causes,certain,certainly,changes,clearly,co,com,come,comes,concerning,consequently,consider,considering,contain,containing,contains," +
                "corresponding,could,couldn't,course,currently,definitely,described,despite,did,didn't,different,do,does,doesn't,doing,don't,done,down,downwards," +
                "during,each,edu,eg,eight,either,else,elsewhere,enough,entirely,especially,et,etc,even,ever,every,everybody,everyone,everything,everywhere,ex,exactly," +
                "example,except,far,few,fifth,first,five,followed,following,follows,for,former,formerly,forth,four,from,further,furthermore,get,gets,getting,given,gives," +
                "go,goes,going,gone,got,gotten,greetings,had,hadn't,happens,hardly,has,hasn't,have,haven't,having,he,he's,hello,help,hence,her,here,here's,hereafter,hereby," +
                "herein,hereupon,hers,herself,hi,him,himself,his,hither,hopefully,how,howbeit,however,i'd,i'll,i'm,i've,ie,if,ignored,immediate,in,inasmuch,inc,indeed,indicate," +
                "indicated,indicates,inner,insofar,instead,into,inward,is,isn't,it,it'd,it'll,it's,its,itself,just,keep,keeps,kept,know,knows,known,last,lately,later,latter,latterly," +
                "least,less,lest,let,let's,like,liked,likely,little,look,looking,looks,ltd,mainly,many,may,maybe,me,mean,meanwhile,merely,might,more,moreover,most,mostly,much,must,my," +
                "myself,name,namely,nd,near,nearly,necessary,need,needs,neither,never,nevertheless,new,next,nine,no,nobody,non,none,noone,nor,normally,not,nothing,novel,now,nowhere," +
                "obviously,of,off,often,oh,ok,okay,old,on,once,one,ones,only,onto,or,other,others,otherwise,ought,our,ours,ourselves,out,outside,over,overall,own,particular,particularly," +
                "per,perhaps,placed,please,plus,possible,presumably,probably,provides,que,quite,qv,rather,rd,re,really,reasonably,regarding,regardless,regards,relatively,respectively,right," +
                "said,same,saw,say,saying,says,second,secondly,see,seeing,seem,seemed,seeming,seems,seen,self,selves,sensible,sent,serious,seriously,seven,several,shall,she,should,shouldn't," +
                "since,six,so,some,somebody,somehow,someone,something,sometime,sometimes,somewhat,somewhere,soon,sorry,specified,specify,specifying,still,sub,such,sup,sure,t's,take,taken,tell," +
                "tends,th,than,thank,thanks,thanx,that,that's,thats,the,their,theirs,them,themselves,then,thence,there,there's,thereafter,thereby,therefore,therein,theres,thereupon,these,they," +
                "they'd,they'll,they're,they've,think,third,this,thorough,thoroughly,those,though,three,through,throughout,thru,thus,to,together,too,took,toward,towards,tried,tries,truly,try,trying," +
                "twice,two,un,under,unfortunately,unless,unlikely,until,unto,up,upon,us,use,used,useful,uses,using,usually,value,various,very,via,viz,vs,want,wants,was,wasn't,way,we,we'd,we'll,we're," +
                "we've,welcome,well,went,were,weren't,what,what's,whatever,when,whence,whenever,where,where's,whereafter,whereas,whereby,wherein,whereupon,wherever,whether,which,while,whither,who,who's," +
                "whoever,whole,whom,whose,why,will,willing,wish,with,within,without,won't,wonder,would,wouldn't,yes,yet,you,you'd,you'll," +
                "you're,you've,your,yours,yourself,yourselves,zero");
        //Initialize Job 1 with the name "Category Frequency"
        Job job1 = Job.getInstance(config, "Category Frequency");
        //Set the Jarclass
        job1.setJarByClass(Qui_Square.class);
        //Set the Mapperclass
        job1.setMapperClass(CategoryMapper.class);
        //Set the Reducerclass
        job1.setReducerClass(CategoryReducer.class);
        //Set the outputkey class
        job1.setOutputKeyClass(Text.class);
        //Set the outputvalue class
        job1.setOutputValueClass(Text.class);
        //Set the outputvalue class of the mapper
        job1.setMapOutputValueClass(DictionaryWritable.class);
        //Specify the path which Job1 reads to
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        // Sets the number of reducers working on the reducer solution. 
        job1.setNumReduceTasks(22);
        String job_1_OutputPath = args[1] + "/job1/";
        //Specify output path of job 1
        FileOutputFormat.setOutputPath(job1, new Path(job_1_OutputPath));
        //Execute job 1
        job1.waitForCompletion(true);
        config = new Configuration();
        config.set("path_job_1",args[1]);

        //Get the counter that was initialized in job1 and put it into the config file
        Object cn=job1.getCounters().findCounter(DOCUMENT_COUNTER.TOTAL_NUMBER_DOCUMENTS).getValue();
        //Initialize job2
        Job job2 = Job.getInstance(config, "Category Word");
        job2.getConfiguration().setLong(DOCUMENT_COUNTER.TOTAL_NUMBER_DOCUMENTS.name(),Long.parseLong(cn.toString()));
        //Set Jarclass
        job2.setJarByClass(Qui_Square.class);
        //Set Mapperclass
        job2.setMapperClass(CategoryWordMapper.class);
        //Set reducer class
        job2.setReducerClass(ReduceCategoryWord.class);
        //Set output class
        job2.setOutputKeyClass(Text.class);
        //Set the outputvalue class
        job2.setOutputValueClass(Text.class);
        //Set the outputvalue class of the mapper
        job2.setMapOutputValueClass(MapWritable.class);
        //set input path for job2
        FileInputFormat.addInputPath(job2, new Path(job_1_OutputPath));
        String job_2_OutputPath = args[1] + "/job2/";
        //Set output path for job 2
        FileOutputFormat.setOutputPath(job2, new Path(job_2_OutputPath));
        //Execute Job2
        job2.waitForCompletion(true);
        //Initialize job3
        Job job3 = Job.getInstance(config, "Orger Merge");
        //Set jar class
        job3.setJarByClass(Qui_Square.class);
        //set mapper class
        job3.setMapperClass(OrderMergeMapper.class);
        //set reducer class
        job3.setReducerClass(OrderMergeReducer.class);
        //set output key class
        job3.setOutputKeyClass(Text.class);
        //set outpuvalue class
        job3.setOutputValueClass(Text.class);
        //set outpuvalue class for mapper
        job3.setMapOutputValueClass(MapWritable.class);
        //set input path for job3
        FileInputFormat.addInputPath(job3, new Path(job_2_OutputPath));
        //set output path for job3
        String job_3_OutputPath = args[1] + "/job3/";
        FileOutputFormat.setOutputPath(job3, new Path(job_3_OutputPath));
        //execute job3
        System.exit(job3.waitForCompletion(true) ? 0 : 1);

    }
}



