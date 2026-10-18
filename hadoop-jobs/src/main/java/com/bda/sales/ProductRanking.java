package com.bda.sales;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProductRanking extends org.apache.hadoop.conf.Configured implements Tool {
    public static class RankingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable offset, Text input, Context context) throws IOException, InterruptedException {
            String[] columns = input.toString().split("\\t", -1);
            if (columns.length != 8 || !"AGG".equals(columns[0]) || !"PRODUCT".equals(columns[1])) return;
            try {
                String productId = Csv.decode(columns[2]);
                long quantity = Long.parseLong(columns[3]);
                long revenueCents = Long.parseLong(columns[4]);
                emit(context, "REVENUE", revenueCents, productId, quantity, revenueCents);
                emit(context, "QUANTITY", quantity, productId, quantity, revenueCents);
            } catch (RuntimeException error) {
                throw new IOException("Invalid aggregation record at input offset " + offset.get(), error);
            }
        }

        private void emit(Context context, String metric, long sortValue, String productId, long quantity, long revenue)
                throws IOException, InterruptedException {
            String inverse = String.format(Locale.ROOT, "%019d", Long.MAX_VALUE - sortValue);
            context.write(new Text(metric + "\t" + inverse + "\t" + productId),
                    new Text(Csv.encode(productId) + "\t" + quantity + "\t" + revenue));
        }
    }

    public static class RankingReducer extends Reducer<Text, Text, Text, Text> {
        private String previousMetric = "";
        private long rank = 0;

        @Override
        protected void reduce(Text sortKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyColumns = sortKey.toString().split("\\t", 3);
            String metric = keyColumns[0];
            if (!metric.equals(previousMetric)) {
                previousMetric = metric;
                rank = 0;
            }
            for (Text value : values) {
                String[] columns = value.toString().split("\\t", -1);
                rank++;
                context.write(new Text(metric + "\t" + String.format(Locale.ROOT, "%09d", rank) + "\t" + columns[0]),
                        new Text(columns[1] + "\t" + columns[2]));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Usage: ProductRanking <aggregation-output> <output>");
        Job job = Job.getInstance(getConf(), "ecommerce-product-ranking");
        job.setJarByClass(ProductRanking.class);
        job.setMapperClass(RankingMapper.class);
        job.setReducerClass(RankingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(args[0]));
        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ProductRanking(), args));
    }
}