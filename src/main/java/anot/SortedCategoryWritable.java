package anot;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable для хранения категории и количества для стадии сортировки.
 */
public class SortedCategoryWritable implements Writable {
    private String catName;
    private int itemsSold;

    public SortedCategoryWritable() {
    }

    public SortedCategoryWritable(String catName, int itemsSold) {
        this.catName = catName;
        this.itemsSold = itemsSold;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(catName);
        out.writeInt(itemsSold);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.catName = in.readUTF();
        this.itemsSold = in.readInt();
    }

    public String getCatName() {
        return catName;
    }

    public int getItemsSold() {
        return itemsSold;
    }
}