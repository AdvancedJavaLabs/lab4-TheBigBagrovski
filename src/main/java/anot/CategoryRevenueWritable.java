package anot;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable для хранения информации о суммарной выручке и количестве.
 */
public class CategoryRevenueWritable implements Writable {
    private double totalIncome;
    private int totalCount;

    public CategoryRevenueWritable() {
    }

    public CategoryRevenueWritable(double totalIncome, int totalCount) {
        this.totalIncome = totalIncome;
        this.totalCount = totalCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(totalIncome);
        out.writeInt(totalCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalIncome = in.readDouble();
        totalCount = in.readInt();
    }

    public double getTotalIncome() {
        return totalIncome;
    }

    public int getTotalCount() {
        return totalCount;
    }
}