// -*- coding: utf-8 -*-
%%{

machine resp;

action mark {
  mark = fpc;
}

action delimted_reply {
  emit(new String(Arrays.copyOfRange(data, mark, fpc - 1)));
}

action integer_reply {
  emit(Long.parseLong(new String(Arrays.copyOfRange(data, mark, fpc - 1))));
}

action empty_bulk_reply {
  emit("");
}

action null_bulk_reply {
  emitNull();
}

action bulk_header_start {
  bulkLength = (int)Long.parseLong(new String(Arrays.copyOfRange(data, mark, fpc)));
}

action bulk_reply_content_start {
  mark = fpc;
  fexec fpc + bulkLength + 1;
}

action bulk_reply {
  emit(new String(Arrays.copyOfRange(data, mark, mark + bulkLength)));
}

action push_empty_array {
  emit(EMPTY_ARRAY);
}

action push_null_array {
  emitNull();
}

action start_array {
  int len = (int)Long.parseLong(new String(Arrays.copyOfRange(data, mark, fpc - 1)));
  ArrayContainer arr = new ArrayContainer(len, currentParent);
  emit(arr);
}

crlf               = "\r\n";

# RESP Errors or Simple Strings
resp_delimited     = ("+" | "-") ( any* -- crlf ) >mark crlf @delimted_reply;

# RESP Integers
resp_integer       = ":" ("-"? digit+) >mark crlf @integer_reply;

# RESP Bulk Strings
resp_bulk_empty    = "0" crlf{2} >empty_bulk_reply;
resp_bulk_nil      = "-1" crlf >null_bulk_reply;
resp_bulk_content  = ([1-9] digit*) >mark crlf >bulk_header_start %*bulk_reply_content_start crlf >bulk_reply;
resp_bulk          = "$" (resp_bulk_empty | resp_bulk_nil | resp_bulk_content);

# RESP Arrays
resp_array_header  = "*" ( ("-1" >mark crlf @push_null_array) | (digit+ >mark crlf @start_array) );

main := (resp_delimited | resp_integer | resp_bulk | resp_array_header)+ %eof{ parseState = PARSE_COMPLETE; };

}%%

package redis.protocol;

import java.util.*;

public class ReplyParser {
  // heavily inspired by this great tutorial
  // http://www.avrfreaks.net/forum/codec-parsing-json-based-config-file-using-micro-memory-ragel
  public static final int PARSE_NOT_STARTED = -1;
  public static final int PARSE_ERROR       = -2;
  public static final int PARSE_OK          = 0;
  public static final int PARSE_INCOMPLETE  = 1;
  public static final int PARSE_COMPLETE    = 2;

  public static final ArrayList<Object> EMPTY_ARRAY = new ArrayList<Object>();

  // parser state
  public final ArrayContainer root;
  private ArrayContainer currentParent;
  private int mark = 0;
  private int bulkLength  = -1;
  private int parseState  = PARSE_NOT_STARTED;

  // incremental state
  private int discardMarker = 0;
  private byte[] previousBuffer = null;

  // ragel members
  private int p;
  private int eof;
  private int pe;
  private int te;
  private int ts;
  private int cs;
  private int act;

  public static class ArrayContainer extends ArrayList<Object> {
    public final int length;
    public final ArrayContainer parent;

    public ArrayContainer(int length, ArrayContainer parent) {
      super(length);
      this.length = length;
      this.parent = parent;
    }

    public ArrayContainer(int length) {
      super(length);
      this.length = length;
      this.parent = null;
    }

    public boolean isFull() {
      return size() >= length;
    }

    public boolean addItem(Object value) {
      if (isFull()) {
        throw new RuntimeException("Container is full!");
      }

      return add(value);
    }

    public String toString() {
      if (parent == null) {
          return  "<Root length: " + length + ", " + super.toString() + ">";
      } else {
        return  "<Child length: " + length + ", " + super.toString() + ">";
      }
    }
  }

  %% write data;

  public ReplyParser() {
    this(1);
  }

  public ReplyParser(int replies) {
    %% write init;
    root = new ArrayContainer(1);
    currentParent = root;
  }

  public static void println(String s) {
    System.out.println(s);
  }

  public static void print(String s) {
    System.out.print(s);
  }

  public void printchars(byte[] data, int start, int stop) {
    print("|");
    for (int i = 0; i < stop - start; i++) {
      char ch = (char)data[start + i];
      String s = null;

      switch(ch) {
        case '\r': s = "\\r"; break;
        case '\n': s = "\\n"; break;
        default:
          s = " " + Character.toString(ch);
      }

      print(" " + s + " |");
    }

    print("\n");

    for (int i = 0; i < stop - start; i++) {
      String s = "  ";
      if (mark == (start + i)) {
        s = "  ^";
      } else if (ts == (start + i)) {
        s = "  *";
      }
      print(" " + s + " ");
    }

    print("\n");
  }

  public void logReply() {
    println(root.toString());
  }

  private void popMultiBulk() {
    if (currentParent.isFull() && currentParent.parent != null) {
      currentParent = currentParent.parent;
      popMultiBulk();
    }
  }

  public void emit(ArrayContainer newMultiBulk) {
    currentParent.addItem(newMultiBulk);
    currentParent = newMultiBulk;
    popMultiBulk();
  }

  public void emit(Object value) {
    currentParent.addItem(value);
    popMultiBulk();
  }

  public void emitNull() {
    currentParent.addItem(null);
    popMultiBulk();
  }

  public Object parse(byte[] buffer) {
    byte[] data = null;

    if (previousBuffer != null) {
      data = new byte[previousBuffer.length + buffer.length];
      System.arraycopy(previousBuffer, 0, data, 0, previousBuffer.length);
      System.arraycopy(buffer, 0, data, previousBuffer.length, buffer.length);

    } else {
      data = buffer;
      p = 0;
      parseState = PARSE_INCOMPLETE;
    }

    eof = data.length;
    pe  = data.length;

    println("============================");
    println("Parse:\n" + new String(data));

    %% write exec;

    switch(parseState) {
      case PARSE_NOT_STARTED:
        print("PARSE_NOT_STARTED ");
        break;
      case PARSE_ERROR:
        print("PARSE_ERROR ");
        break;
      case PARSE_OK:
        println("PARSE_OK ");
        break;
      case PARSE_INCOMPLETE:
        print("PARSE_INCOMPLETE ");
        previousBuffer = data;
        break;
      case PARSE_COMPLETE:
        print("PARSE_COMPLETE ");
        break;
    }

    logReply();
    println("");

    return parseState;
  }

  public Object parse(String data) {
    return parse(data.getBytes());
  }

  public static void main(String[] args) {
    // error
    // new ReplyParser().parse("-Error message\r\n");

    // new ReplyParser().parse("-Error incomplete");

    println("=== Incremental ===");
    ReplyParser incremental = new ReplyParser();
    println("" + incremental.parse("-ERR"));
    println("" + incremental.parse("\r"));
    println("" + incremental.parse("\n"));

    new ReplyParser().parse("ERR wrong number of arguments for 'get' command\r\n");

    // status
    new ReplyParser().parse("+OK\r\n".getBytes());
    new ReplyParser().parse("+OK_INCOMPLETE\r");

    // integer
    new ReplyParser().parse(":1000\r\n");
    new ReplyParser().parse(":-1\r\n");

    // bulk strings
    new ReplyParser().parse("$6\r\nfoobar\r\n");
    new ReplyParser().parse("$-1\r\n");
    new ReplyParser().parse("$0\r\n\r\n");

    // array
    new ReplyParser().parse("*0\r\n");
    new ReplyParser().parse("*-1\r\n");
    new ReplyParser().parse("*3\r\n:1\r\n:2\r\n:3\r\n");
    new ReplyParser().parse("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n");
    new ReplyParser().parse("*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n");
  }
}
