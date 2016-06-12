package redis.resp;


public class Error {
    public final String message;

    public Error(final String msg) {
        if (msg == null) {
            throw new IllegalArgumentException("message cannot be null!");
        }
        this.message = msg;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        return (obj instanceof Error && this.message.equals(((Error)obj).message));
    }

    public int hashCode() {
        int hash = 7;
        hash = 13 * this.message.hashCode();
        return hash;
    }

    public String toString() {
        return "-" + message;
    }
}
