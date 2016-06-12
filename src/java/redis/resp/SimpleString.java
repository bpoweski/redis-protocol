package redis.resp;


public class SimpleString {
    public final String message;

    public SimpleString(final String msg) {
        if (msg == null) {
            throw new IllegalArgumentException("message cannot be null!");
        }
        this.message = msg;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        return (obj instanceof SimpleString && this.message.equals(((SimpleString)obj).message));
    }

    public int hashCode() {
        int hash = 13;
        hash = 17 * this.message.hashCode();
        return hash;
    }

    public String toString() {
        return "+" + message;
    }
}
