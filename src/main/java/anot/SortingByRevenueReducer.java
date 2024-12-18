package anot;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Редьюсер окончательной сортировки по выручке
 */
public class SortingByRevenueReducer extends Reducer<DoubleWritable, SortedCategoryWritable, Text, Text> {
    @Override
    protected void reduce(DoubleWritable negRevenue, Iterable<SortedCategoryWritable> vals, Context context) throws IOException, InterruptedException {
        double originalRevenue = -1.0 * negRevenue.get();
        for (SortedCategoryWritable record : vals) {
            String c = record.getCatName();
            int q = record.getItemsSold();
            context.write(new Text(c), new Text(String.format("%.2f\t%d", originalRevenue, q)));
        }
    }
}