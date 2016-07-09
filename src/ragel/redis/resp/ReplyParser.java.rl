// -*- coding: utf-8 -*-
%%{

machine resp;

action mark {
  mark = fpc;
}

action delimted_reply {
  String delimitedValue = new String(Arrays.copyOfRange(data, mark, fpc - 1));

  if (delimitedReplyType == ERROR_REPLY) {
    emit(new Error(delimitedValue));
  } else {
    emit(new SimpleString(delimitedValue));
  }
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
}

action bulk_reply_body {
  println("check: " + (fpc - mark) + " " + (char)fc);

  if ((fpc - mark + 1) == bulkLength) {
    println("read: " + (fpc - mark + 1) + " chars, exiting");
    fnext resp_end_of_bulk;
  }
}


action bulk_reply {
  emit(Arrays.copyOfRange(data, mark, mark + bulkLength));
}

action push_empty_array {
  emit(EMPTY_ARRAY);
}

action push_null_array {
  emitNull();
}

action set_simple {
  delimitedReplyType = SIMPLE_STRING_REPLY;
}

action set_error {
  delimitedReplyType = ERROR_REPLY;
}

action start_array {
  int len = (int)Long.parseLong(new String(Arrays.copyOfRange(data, mark, fpc - 1)));
  Array arr = new Array(len, currentParent);
  emit(arr);
}

action check_end_of_reply {
  if (p == pe && pe == eof && root.isComplete()) {
    parseState = PARSE_COMPLETE;
  } else if (currentParent == root && root.isFull()) {
    parseState = PARSE_OVERFLOW;
    fhold;
    fbreak;
  }
}

crlf               = "\r\n";

# RESP Errors or Simple Strings
resp_delimited     = ("+" >set_simple | "-" >set_error) ( any* -- crlf ) >mark crlf @delimted_reply;

# RESP Integers
resp_integer       = ":" ("-"? digit+) >mark crlf @integer_reply;

# RESP Bulk Strings
resp_bulk_empty    = "0" crlf{2} >empty_bulk_reply;
resp_bulk_nil      = "-1" crlf >null_bulk_reply;
resp_end_of_bulk   = crlf;
resp_bulk_content  = ([1-9] digit*) >mark crlf >bulk_header_start %*bulk_reply_content_start (any >bulk_reply_body)* resp_end_of_bulk %bulk_reply;
resp_bulk          = "$" (resp_bulk_empty | resp_bulk_nil | resp_bulk_content);

# RESP Arrays
resp_array_header  = "*" ( ("-1" >mark crlf @push_null_array) | (digit+ >mark crlf @start_array) );

main := ( (resp_delimited | resp_integer | resp_bulk | resp_array_header) %check_end_of_reply)+;

}%%

package redis.resp;

import java.util.*;
import java.util.logging.Logger;
import redis.resp.SimpleString;
import redis.resp.Array;
import redis.resp.Error;

public class ReplyParser {
  private static Logger LOG = Logger.getLogger("redis.resp.ReplyParser");

  // heavily inspired by this great tutorial
  // http://www.avrfreaks.net/forum/codec-parsing-json-based-config-file-using-micro-memory-ragel
  public static final int PARSE_NOT_STARTED = -1;
  public static final int PARSE_ERROR       = -2;
  public static final int PARSE_OK          = 0;
  public static final int PARSE_INCOMPLETE  = 1;
  public static final int PARSE_COMPLETE    = 2;
  public static final int PARSE_OVERFLOW    = 3;

  public static final int SIMPLE_STRING_REPLY  = 0;
  public static final int ERROR_REPLY          = 1;

  public static final ArrayList<Object> EMPTY_ARRAY = new ArrayList<Object>();

  // parser state
  public final Array root;
  private Array currentParent;
  private int mark = 0;
  private int bulkLength  = -1;
  private int parseState  = PARSE_NOT_STARTED;
  private byte[] data = null;
  private int delimitedReplyType = -1;

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

  %% write data;

  public ReplyParser() {
    this(1);
  }

  public ReplyParser(int replies) {
    %% write init;
    root = new Array(replies);
    currentParent = root;
  }

  public static void println(String s) {
    // System.out.println(s);
  }

  public static void print(String s) {
    // System.out.print(s);
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

  public void emit(Array newMultiBulk) {
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

  public byte[] getOverflow() {
    byte[] overflow = new byte[pe - p];
    System.arraycopy(data, p, overflow, 0, overflow.length);
    return overflow;
  }

  public Object parse(byte[] buffer) {
    if (data != null) {
      byte[] previousBuffer = data;
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
      case PARSE_OVERFLOW:
        println("PARSE_OVERFLOW ");
        break;
      case PARSE_INCOMPLETE:
        print("PARSE_INCOMPLETE ");
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

  public static void main(String[] args) throws Exception {
    ReplyParser parser = new ReplyParser(1);
    LOG.info("parser.parse: " + parser.parse(java.nio.file.Files.readAllBytes(new java.io.File("/Users/bpoweski/Projects/getaroom/redis-protocol/private_scripts/response.bin").toPath())));

    // error
    // new ReplyParser().parse("-Error message\r\n");

    // new ReplyParser().parse("-Error incomplete");

    // println("=== Incremental ===");
    // ReplyParser incremental = new ReplyParser();
    // println("" + incremental.parse("-ERR"));
    // println("" + incremental.parse("\r"));
    // println("" + incremental.parse("\n"));


    // println("=== Incremental String ===");
    // ReplyParser incrementalString = new ReplyParser();
    // println("" + incrementalString.parse("$52\r\n"));
    // println("" + incrementalString.parse(":20160705:\r\n160705:T::DBL:CV-DX"));
    // println("" + incrementalString.parse("::2:100:Y:Y:Y:Y:Y:Y:Y\r\n"));
    // println("" + new String((byte[])incrementalString.root.get(0)));

    // new ReplyParser().parse("-ERR wrong number of arguments for 'get' command\r\n");
    //
    // // status
    // new ReplyParser().parse("+OK\r\n".getBytes());
    // new ReplyParser().parse("+OK_INCOMPLETE\r");
    // ReplyParser overflow = new ReplyParser();
    // overflow.parse("+OK\r\n+NEXT\r");
    // byte[] overflowBytes = overflow.getOverflow();
    //
    // // integer
    // new ReplyParser().parse(":1000\r\n");
    // new ReplyParser().parse(":-1\r\n");
    //
    // // bulk strings
    // new ReplyParser().parse("$6\r\nfoobar\r\n");
    // new ReplyParser().parse("$-1\r\n");
    // new ReplyParser().parse("$0\r\n\r\n");
    //
    // // array
    // new ReplyParser().parse("*0\r\n");
    // new ReplyParser().parse("*-1\r\n");
    // new ReplyParser().parse("*3\r\n:1\r\n:2\r\n:3\r\n");
    // new ReplyParser().parse("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n");
    // new ReplyParser().parse("*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n");
  }
}
