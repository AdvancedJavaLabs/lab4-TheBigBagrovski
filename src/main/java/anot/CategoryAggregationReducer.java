package anot;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Редьюсер для суммирования выручки и количества.
 */
public class CategoryAggregationReducer extends Reducer<Text, CategoryRevenueWritable, Text, Text> {
    @Override
    protected void reduce(Text category, Iterable<CategoryRevenueWritable> records, Context ctxt) throws IOException, InterruptedException {
        double sumRevenue = 0.0;
        int sumQuantity = 0;

        for (CategoryRevenueWritable entry : records) {
            sumRevenue += entry.getTotalIncome();
            sumQuantity += entry.getTotalCount();
        }

        ctxt.write(category, new Text(String.format("%.2f\t%d", sumRevenue, sumQuantity)));
    }
}