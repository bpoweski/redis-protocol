package redis.resp;

import java.util.ArrayList;

public class Array extends ArrayList<Object> {
    public final int length;
    public final Array parent;

    public Array(int length, Array parent) {
        super(length);
        this.length = length;
        this.parent = parent;
    }

    public Array(int length) {
        super(length);
        this.length = length;
        this.parent = null;
    }

    public boolean isFull() {
        return size() >= length;
    }

    public boolean isComplete() {
        if (!isFull()) {
            return false;
        }

        for (Object x : this) {
            if (x instanceof Array) {
                Array arrayElement = (Array)x;

                if (!arrayElement.isComplete()) {
                    return false;
                }
            }
        }

        return true;
    }

    public boolean addItem(Object value) {
        if (isFull()) {
            throw new RuntimeException("Container is full!");
        }

        return add(value);
    }
}
