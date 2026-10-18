package com.bda.sales;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SalesAggregation extends org.apache.hadoop.conf.Configured implements Tool {
    public static class SalesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable offset, Text input, Context context) throws IOException, InterruptedException {
            List<String> row;
            try {
                row = Csv.parse(input.toString());
                if (row.isEmpty() || "order_id".equals(row.get(0))) return;
                if (row.size() < 8) throw new IllegalArgumentException("Expected at least 8 transaction fields");
                long quantity = Long.parseLong(row.get(4));
                long unitPriceCents = new BigDecimal(row.get(5)).movePointRight(2).longValueExact();
                long revenueCents = Math.multiplyExact(quantity, unitPriceCents);
                String orderId = Csv.encode(row.get(0));
                String values = orderId + "\t" + quantity + "\t" + revenueCents;
                String month = row.get(1).substring(0, 7);
                emit(context, "PRODUCT", row.get(3), values);
                emit(context, "CATEGORY", row.get(7), values);
                emit(context, "REGION", row.get(6), values);
                emit(context, "MONTH", month, values);
                emit(context, "TOTAL", "ALL", values);
            } catch (RuntimeException error) {
                throw new IOException("Invalid validated transaction at input offset " + offset.get(), error);
            }
        }

        private void emit(Context context, String type, String key, String value) throws IOException, InterruptedException {
            context.write(new Text("AGG\t" + type + "\t" + Csv.encode(key)), new Text(value));
        }
    }

    public static class SalesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> orders = new HashSet<>();
            long quantity = 0;
            long revenueCents = 0;
            long lineCount = 0;
            for (Text value : values) {
                String[] columns = value.toString().split("\\t", -1);
                if (columns.length != 3) throw new IOException("Malformed mapper value");
                orders.add(columns[0]);
                quantity = Math.addExact(quantity, Long.parseLong(columns[1]));
                revenueCents = Math.addExact(revenueCents, Long.parseLong(columns[2]));
                lineCount++;
            }
                long orderCount = orders.size();
                long averageOrderValueCents = orderCount == 0 ? 0 :
                    BigDecimal.valueOf(revenueCents).divide(BigDecimal.valueOf(orderCount), 0, java.math.RoundingMode.HALF_UP).longValueExact();
                context.write(key, new Text(quantity + "\t" + revenueCents + "\t" + orderCount + "\t" + lineCount
                    + "\t" + averageOrderValueCents));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Usage: SalesAggregation <input> <output>");
        Job job = Job.getInstance(getConf(), "ecommerce-sales-aggregation");
        job.setJarByClass(SalesAggregation.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new SalesAggregation(), args));
    }
}