package com.bda.sales;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProductJoin extends org.apache.hadoop.conf.Configured implements Tool {
    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable offset, Text input, Context context) throws IOException, InterruptedException {
            List<String> row = Csv.parse(input.toString());
            if (row.isEmpty() || "order_id".equals(row.get(0))) return;
            try {
                String value = "T\t" + Csv.encode(row.get(0)) + "\t" + row.get(1) + "\t" + row.get(4) + "\t"
                        + new BigDecimal(row.get(5)).movePointRight(2).longValueExact();
                context.write(new Text(row.get(3)), new Text(value));
            } catch (RuntimeException error) {
                throw new IOException("Invalid transaction at input offset " + offset.get(), error);
            }
        }
    }

    public static class ProductMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable offset, Text input, Context context) throws IOException, InterruptedException {
            List<String> row = Csv.parse(input.toString());
            if (row.isEmpty() || "product_id".equals(row.get(0))) return;
            if (row.size() < 3) throw new IOException("Product lookup requires product_id, product_name, and category");
            String cost = "";
            if (row.size() > 3 && !row.get(3).isEmpty()) {
                try {
                    cost = Long.toString(new BigDecimal(row.get(3)).movePointRight(2).longValueExact());
                } catch (RuntimeException error) {
                    throw new IOException("Invalid unit_cost at input offset " + offset.get(), error);
                }
            }
            context.write(new Text(row.get(0)), new Text("P\t" + Csv.encode(row.get(1)) + "\t" + Csv.encode(row.get(2)) + "\t" + cost));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private long totalProfitCents;
        private long totalProfitLines;

        @Override
        protected void reduce(Text productId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String productName = "";
            String category = "";
            String unitCostCents = "";
            List<String[]> transactions = new ArrayList<>();
            for (Text value : values) {
                String[] columns = value.toString().split("\\t", -1);
                if ("P".equals(columns[0])) {
                    productName = columns[1];
                    category = columns[2];
                    unitCostCents = columns[3];
                } else if ("T".equals(columns[0])) {
                    transactions.add(columns);
                }
            }
            String encodedProduct = Csv.encode(productId.toString());
            long productProfitCents = 0;
            long productProfitLines = 0;
            for (String[] transaction : transactions) {
                String profit = "";
                if (!unitCostCents.isEmpty()) {
                    long lineProfit = Math.multiplyExact(Long.parseLong(transaction[3]),
                            Long.parseLong(transaction[4]) - Long.parseLong(unitCostCents));
                    productProfitCents = Math.addExact(productProfitCents, lineProfit);
                    productProfitLines++;
                    profit = Long.toString(lineProfit);
                }
                context.write(new Text("JOIN\t" + encodedProduct), new Text(transaction[1] + "\t" + transaction[2] + "\t"
                        + transaction[3] + "\t" + transaction[4] + "\t" + productName + "\t" + category + "\t"
                    + unitCostCents + "\t" + profit + "\t" + (productName.isEmpty() ? "MISSING" : "MATCHED")));
            }
            if (productProfitLines > 0) {
                totalProfitCents = Math.addExact(totalProfitCents, productProfitCents);
                totalProfitLines = Math.addExact(totalProfitLines, productProfitLines);
                context.write(new Text("PROFIT\t" + encodedProduct),
                        new Text(productProfitCents + "\t" + productProfitLines));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (totalProfitLines > 0) {
                context.write(new Text("PROFIT\tALL"), new Text(totalProfitCents + "\t" + totalProfitLines));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("Usage: ProductJoin <orders> <products> <output>");
        Job job = Job.getInstance(getConf(), "ecommerce-product-lookup-join");
        job.setJarByClass(ProductJoin.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransactionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ProductMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ProductJoin(), args));
    }
}