package anot;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Маппер для подготовки данных к сортировке:
 * Меняем знак выручки на отрицательный, чтобы при сортировке по возрастанию
 * получить убывание дохода.
 */
public class SortingPreparationMapper extends Mapper<Object, Text, DoubleWritable, SortedCategoryWritable> {
    private final DoubleWritable negativeRevenue = new DoubleWritable();

    @Override
    protected void map(Object byteOffset, Text line, Context ctx) throws IOException, InterruptedException {
        // category \t revenue \t quantity
        String[] arr = line.toString().split("\t");
        if (arr.length == 3) {
            String cat = arr[0].trim();
            double rev = Double.parseDouble(arr[1].trim());
            int qnt = Integer.parseInt(arr[2].trim());

            negativeRevenue.set(-1.0 * rev);
            ctx.write(negativeRevenue, new SortedCategoryWritable(cat, qnt));
        }
    }
}