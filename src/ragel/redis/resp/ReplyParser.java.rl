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
  emit(null);
}

action bulk_length {
  bulkLength = (int)Long.parseLong(new String(Arrays.copyOfRange(data, mark, fpc + 1)));
  minimumBufferSize = Math.max(fpc + 2 + bulkLength + 2, pe);
}

action start_bulk {
  mark = fpc;
}

action bulk_skip {
  if ((fpc - mark) == (bulkLength - 1)) {
    fnext resp_bulk_end;
  } else {
    fexec Math.min(fpc + bulkLength - 2, pe);
  }
}

action bulk_reply {
  emit(Arrays.copyOfRange(data, mark, mark + bulkLength));
}

action push_empty_array {
  emit(new Array(0, currentParent));
}

action push_null_array {
  emit(null);
}

action push_array {
  int len = (int)Long.parseLong(new String(Arrays.copyOfRange(data, mark, fpc - 1)));
  Array arr = new Array(len, currentParent);
  emit(arr);
}

action check_if_reply_complete {
  if ((fpc + 1) == pe && isDone()) {
    parseState = PARSE_COMPLETE;
  } else if ((fpc + 1) < pe) {
    if (isDone()) {
      parseState = PARSE_OVERFLOW;
      fbreak;
    } else {
      fgoto reply;
    }
  }
}

crlf                        = "\r\n";

# RESP Simple
resp_simple                 := "+" (any* -- crlf) >mark crlf @simple_reply @check_if_reply_complete;

# RESP Error
resp_error                  := "-" (any* -- crlf) >mark crlf @error_reply @check_if_reply_complete;

# RESP Integers
resp_integer                := ":" ("-"? digit+) >mark crlf @integer_reply @check_if_reply_complete;

# RESP Bulk Strings
resp_bulk_nil               = "-1" crlf @null_bulk_reply;
resp_bulk_empty             = "0" crlf{2} @empty_bulk_reply;
resp_bulk_end               = crlf; # used for fnext
resp_bulk_content           = ([1-9] digit*) >mark @bulk_length crlf any >start_bulk (any >bulk_skip)* resp_bulk_end @bulk_reply;
resp_bulk                   := "$" (resp_bulk_empty | resp_bulk_nil | resp_bulk_content) @check_if_reply_complete;

# RESP Arrays
resp_null_array             = "-1" crlf;
resp_empty_array            = "0" crlf;
resp_non_empty_array_header = ([1-9] digit*) >mark crlf @push_array;
resp_non_empty_array        = resp_non_empty_array_header @{ fgoto reply; };
resp_array                  := "*" (resp_null_array @push_null_array | resp_empty_array @push_empty_array | resp_non_empty_array_header) @check_if_reply_complete;

reply                       := ('+' @{ fhold; fgoto resp_simple;   } |
                                '-' @{ fhold; fgoto resp_error;    } |
                                ':' @{ fhold; fgoto resp_integer;  } |
                                '$' @{ fhold; fgoto resp_bulk;     } |
                                '*' @{ fhold; fgoto resp_array;    });

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
  private Array currentParent = null;
  private int mark            = 0;
  private int bulkLength      = -1;
  private int bulkConsumed    = 0;

  private int parseState         = PARSE_NOT_STARTED;
  private byte[] data            = null;
  private int delimitedReplyType = -1;

  // buffer hint
  private int minimumBufferSize = -1;

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

  private void unwind() {
    while (currentParent != null && currentParent.isFull()) {
      currentParent = currentParent.parent;
    }
  }

  public boolean isDone() {
    return currentParent == null && root.isComplete();
  }

  public void emit(Object value) {
    currentParent.addItem(value);
    unwind();
  }

  public void emit(Array newMultiBulk) {
    currentParent.addItem(newMultiBulk);

    if (newMultiBulk != null) {
      currentParent = newMultiBulk;
    }

    unwind();
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

    int parseState = -1;
    println("=====");
    for (int i = 0; i < args.length; i++) {
        println("Input: " + args[i]);
        parseState = (int)parser.parse(args[i]);
        println("" + parseState);
    }

    if (parseState == 3) {
      println("Overflow:");
      byte[] overflow = parser.getOverflow();
      parser.printchars(overflow, 0, overflow.length);
    }
  }
}
