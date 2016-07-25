// -*- coding: utf-8 -*-
%%{

machine resp;

action mark {
  mark = fpc;
}

action simple_reply {
  String delimitedValue = new String(Arrays.copyOfRange(data, mark, fpc - 1));
  emit(new SimpleString(delimitedValue));
}

action error_reply {
  String delimitedValue = new String(Arrays.copyOfRange(data, mark, fpc - 1));
  emit(new Error(delimitedValue));
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

#// |  1 |  0 | \r | \n |  B |  O  | ...
#//    M         ^
action bulk_header_start {
  bulkLength = (int)Long.parseLong(new String(Arrays.copyOfRange(data, mark, fpc)));

  // hint the minimum size buffer needed to satisfy everything up-to the current bulk string for later incremental parsing
  minimumBufferSize = Math.max(fpc + 2 + bulkLength + 2, pe);
}

action bulk_reply_content_start {
  mark = fpc;
}

action bulk_reply_body {
  int consumed = (fpc - mark + 1);
  int available = (pe - fpc);

  // println("check: " + (fpc - mark) + " " + (char)fc + " pe: " + pe + ", fpc: " + fpc + ", available: " + available);


  if (consumed == bulkLength) {
    println("read: " + (fpc - mark + 1) + " chars, exiting");

    if (root == currentParent) {
        fnext ::main::resp_item::resp_bulk::resp_bulk_content::resp_end_of_bulk;
    } else {
        fnext ::resp_array_items::resp_item::resp_bulk::resp_bulk_content::resp_end_of_bulk;
    }
  }

  // if (consumed == bulkLength) {
  //   //println("read: " + (fpc - mark + 1) + " chars, exiting");
  //   fnext resp_end_of_bulk;
  // } else if ((bulkLength - consumed) == 1 || available == 1) {
  //   //println("only one char to consume");
  // } else {
  //   //println("jumping ahead to: " + Math.min(pe - 1, (mark + bulkLength - 1)));
  //   fexec Math.min(pe - 1, (mark + bulkLength - 1));
  // }
}


action bulk_reply {
  println("bulk reply");
  emit(Arrays.copyOfRange(data, mark, mark + bulkLength));
}

action push_empty_array {
  emit(EMPTY_ARRAY);
}

action push_null_array {
  emitNull();
}

action push_array {
  int len = (int)Long.parseLong(new String(Arrays.copyOfRange(data, mark, fpc - 1)));
  Array arr = new Array(len, currentParent);
  emit(arr);
}

action check_if_complete {
  if (currentParent.isComplete()) {
    println("check_if_complete: true");
    popArray();
    fret;
  } else {
    println("check_if_complete: false");
  }
}

action complete {
  if (currentParent.isFull()) {
    println("complete:");
    parseState = PARSE_COMPLETE;
    fnext overflow;
  }
}


action check_if_finished {
  if (currentParent == root && root.isComplete()) {
      println("check_if_finished: true");
      parseState = PARSE_COMPLETE;
  } else {
      println("check_if_finished: false");
  }
}

action debug {
  println("debug: fpc: " + fpc + ", pe: " + pe);
  printchars(data, fpc, pe);
  println(root.toString());
}

postpop {
  println("postpop: fpc: " + fpc + ", pe: " + pe);
  printchars(data, fpc, pe);
  println(root.toString());

  if (currentParent == root && root.isComplete()) {
      println("check_if_finished: true");
      parseState = PARSE_COMPLETE;
  } else {
      println("check_if_finished: false");
  }
}

crlf                        = "\r\n";

# RESP Simple
resp_simple                 = "+" ((any* -- crlf) >mark crlf @simple_reply);

# RESP Error
resp_error                  = "-" (any* -- crlf) >mark crlf @error_reply;

# RESP Integers
resp_integer                = ":" ("-"? digit+) >mark crlf @integer_reply;

# RESP Bulk Strings
resp_bulk_nil               = "-1" crlf @null_bulk_reply;
resp_bulk_empty             = "0" crlf{2} @empty_bulk_reply;
resp_end_of_bulk            = crlf;
resp_bulk_content           = (([1-9] digit*) >mark crlf >bulk_header_start  any >mark (any >bulk_reply_body)* resp_end_of_bulk @bulk_reply);
resp_bulk                   = "$" (resp_bulk_empty | resp_bulk_nil | resp_bulk_content);

# RESP Arrays
resp_null_array             = "-1" crlf @push_null_array;
resp_empty_array            = "0" crlf @push_empty_array;
resp_non_empty_array_header = ([1-9] digit*) >mark crlf @push_array @{ println("new array: "); fcall resp_array_items; };
resp_array_header           = (resp_null_array | resp_empty_array | resp_non_empty_array_header );
resp_array                  = "*" (resp_null_array | resp_empty_array | resp_array_header);


resp_item           = resp_simple  |
                      resp_error   |
                      resp_integer |
                      resp_bulk    |
                      resp_array;


resp_array_items    := (resp_item >debug @check_if_complete)+ ;

overflow            = any*;
main                := (resp_item @complete)+ <: overflow ${ parseState = PARSE_OVERFLOW; fhold; fbreak; };

}%%

package redis.resp;

import java.util.*;
import java.util.logging.Logger;
import redis.resp.SimpleString;
import redis.resp.Array;
import redis.resp.Error;
import java.nio.file.*;

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

  public static final Array EMPTY_ARRAY = new Array(0);

  // parser state
  public final Array root;
  private Array currentParent;
  private int mark = 0;
  private int bulkLength  = -1;
  private int parseState  = PARSE_NOT_STARTED;
  private byte[] data = null;
  private int delimitedReplyType = -1;

  // buffer size hint
  private int minimumBufferSize = -1;


  // ragel members
  private int p;
  private int eof;
  private int pe;
  private int te;
  private int ts;
  private int cs;
  private int act;

  // ragel stack variables
  private int[] stack = new int[7];
  private int top;

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
    //System.out.println(s);
  }

  public static void print(String s) {
    //System.out.print(s);
  }

  public void printchars(byte[] data, int start, int stop) {
    if (!"1".equals(System.getProperty("redis.resp.debug"))) {
      return;
    }

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
    //println(root.toString());
  }

  private void popArray() {
    if (currentParent.isFull() && currentParent.parent != null) {
      currentParent = currentParent.parent;
      popArray();
    }
  }

  public void emit(Array newMultiBulk) {
    currentParent.addItem(newMultiBulk);
    currentParent = newMultiBulk;
  }

  public void emit(Object value) {
    println("adding " + value.toString() + " to " + currentParent.toString() + " with length " + currentParent.length);
    currentParent.addItem(value);
  }

  public void emitNull() {
    currentParent.addItem(null);
  }

  public byte[] getOverflow() {
    byte[] overflow = new byte[pe - p];
    System.arraycopy(data, p, overflow, 0, overflow.length);
    return overflow;
  }

  public Object parse(byte[] buffer) {
    if (data != null) {
      int available = data.length - pe;

      // check for room in the current buffer
      if (available >= buffer.length) {

          System.arraycopy(buffer, 0, data, pe, buffer.length);
          eof += buffer.length;
          pe  += buffer.length;

      } else {

          byte[] previousBuffer = data;
          int previousDataLength = pe;
          int dataLength = previousDataLength + buffer.length;

          int newBufferSize = Math.max(dataLength, minimumBufferSize);

          data = new byte[newBufferSize];

          System.arraycopy(previousBuffer, 0, data, 0, previousDataLength);
          System.arraycopy(buffer, 0, data, previousDataLength, buffer.length);

          eof = dataLength;
          pe  = dataLength;
      }

    } else {

      data              = buffer;
      p                 = 0;
      eof               = data.length;
      pe                = data.length;
      parseState        = PARSE_INCOMPLETE;
      minimumBufferSize = data.length;
    }

    %% write exec;

    return parseState;
  }

  public Object parse(String data) {
    return parse(data.getBytes());
  }


  public static void main(String[] args) throws Exception {
    ReplyParser parser = new ReplyParser(1);
    // println("=====");
    // println("" + parser.parse("+OK\r\n"));
    // parser.logReply();

    // parser = new ReplyParser(1);
    // println("=====");
    // println("" + parser.parse("-MOVED foo\r\n"));
    // parser.logReply();

    // parser = new ReplyParser(1);
    // println("=====");
    // println("" + parser.parse(":100\r\n"));
    // parser.logReply();

    // parser = new ReplyParser(1);
    // println("=====");
    // println("" + parser.parse(":"));
    // println("" + parser.parse("100\r"));
    // println("" + parser.parse("\n"));
    // parser.logReply();

    parser = new ReplyParser(1);
    int parseState = -1;
    println("=====");
    for (int i = 0; i < args.length; i++) {
        println("Input: " + args[i]);
        parseState = (int)parser.parse(args[i]);
        println("" + parseState);
        parser.logReply();
    }

    if (parseState == 3) {
      println("Overflow:");
      byte[] overflow = parser.getOverflow();
      parser.printchars(overflow, 0, overflow.length);
    }

    // parser = new ReplyParser(1);
    // println("=====");
    // println("" + parser.parse("$-1\r\n"));
    // parser.logReply();

    // parser = new ReplyParser(1);
    // String dir = "/Users/bpoweski/Projects/getaroom/redis-protocol/private_scripts";
    // println("" + parser.parse(Files.readAllBytes(Paths.get(dir, "chunk_0.bin"))));
    //println("" + parser.parse(Files.readAllBytes(Paths.get(dir, "chunk_1.bin"))));
    //println("" + parser.parse(Files.readAllBytes(Paths.get(dir, "chunk_2.bin"))));
  }
}
