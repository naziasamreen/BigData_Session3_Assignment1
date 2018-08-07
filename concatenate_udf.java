package hive_udf;

import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDF;

public class concatenate_udf
  extends UDF
{
  public String evaluate(String SEP, ArrayList<String> array)
  {
    if ((SEP == null) || (array == null)) {
      return "Provide a separator followed by an array of column name";
    }
    String element = "";
    for (int i = 0; i < array.size(); i++)
    {
      element = element + (String)array.get(i);
      if (i < array.size() - 1) {
        element = element + SEP;
      }
    }
    return element;
  }
}
