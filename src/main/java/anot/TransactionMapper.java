package anot;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Маппер для чтения транзакций из CSV.
 * Формат входных данных: transaction_id, product_id, category, price, quantity
 */
public class TransactionMapper extends Mapper<Object, Text, Text, CategoryRevenueWritable> {
    private final Text categoryOutKey = new Text();

    @Override
    protected void map(Object offset, Text line, Context ctx) throws IOException, InterruptedException {
        String[] parts = line.toString().split(",");
        if (parts.length == 5 && !"transaction_id".equals(parts[0])) {
            String cat = parts[2].trim();
            double prc = Double.parseDouble(parts[3].trim());
            int qty = Integer.parseInt(parts[4].trim());

            categoryOutKey.set(cat);
            // выручка = price * quantity
            ctx.write(categoryOutKey, new CategoryRevenueWritable(prc * qty, qty));
        }
    }
}