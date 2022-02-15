package util;

import java.text.NumberFormat;
import java.text.ParsePosition;

public class Tools {
    public static boolean isUrlValid(String[] urlPath) {
        if (urlPath.length != 8) {
            return false;
        } else {
            int[] parameterIndex = new int[]{1,3,5,7};
            for(int param : parameterIndex) {
                if (!isNumeric(urlPath[param])) return false;
            }
        }
        return true;
    }

    public static boolean isNumeric(String part) {
        ParsePosition pos = new ParsePosition(0);
        NumberFormat.getInstance().parse(part, pos);
        return part.length() == pos.getIndex();
    }

}
