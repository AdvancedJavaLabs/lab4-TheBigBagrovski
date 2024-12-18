package anot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DataProcessingDriver {
    public static void main(String[] parameters) throws Exception {
        if (parameters.length != 4) {
            System.err.println("Usage: hadoop jar <JAR> "
                    + DataProcessingDriver.class.getName()
                    + " <input> <finalOutput> <reducersCount> <blockSizeKB>");
            System.exit(-1);
        }

        String inputPath = parameters[0];
        String finalOutput = parameters[1];
        int reducers = Integer.parseInt(parameters[2]);
        int blockSizeBytes = Integer.parseInt(parameters[3]) * 1024;

        String interimPath = finalOutput + "-inter";

        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Integer.toString(blockSizeBytes));

        // агрегация выручки по категориям
        Job aggregationJob = Job.getInstance(conf, "Revenue & Quantity Aggregation");
        aggregationJob.setJarByClass(DataProcessingDriver.class);
        aggregationJob.setMapperClass(TransactionMapper.class);
        aggregationJob.setReducerClass(CategoryAggregationReducer.class);

        aggregationJob.setMapOutputKeyClass(Text.class);
        aggregationJob.setMapOutputValueClass(CategoryRevenueWritable.class);
        aggregationJob.setOutputKeyClass(Text.class);
        aggregationJob.setOutputValueClass(Text.class);
        aggregationJob.setNumReduceTasks(reducers);

        FileInputFormat.addInputPath(aggregationJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(aggregationJob, new Path(interimPath));

        boolean completed = aggregationJob.waitForCompletion(true);
        if (!completed) {
            System.err.println("Aggregation step failed.");
            System.exit(1);
        }

        // сортировка результатов по убыванию выручки
        Job sortingJob = Job.getInstance(conf, "Sort by Highest Revenue");
        sortingJob.setJarByClass(DataProcessingDriver.class);
        sortingJob.setMapperClass(SortingPreparationMapper.class);
        sortingJob.setReducerClass(SortingByRevenueReducer.class);

        sortingJob.setMapOutputKeyClass(DoubleWritable.class);
        sortingJob.setMapOutputValueClass(SortedCategoryWritable.class);

        // вывод: category, revenue, quantity
        sortingJob.setOutputKeyClass(Text.class);
        sortingJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortingJob, new Path(interimPath));
        FileOutputFormat.setOutputPath(sortingJob, new Path(finalOutput));

        boolean sortingDone = sortingJob.waitForCompletion(true);

        long finish = System.currentTimeMillis();
        System.out.println("All tasks finished in: " + (finish - start) + " ms.");

        System.exit(sortingDone ? 0 : 1);
    }
}
