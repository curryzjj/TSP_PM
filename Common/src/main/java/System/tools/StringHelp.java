package System.tools;

public class StringHelp {
    public static int isDigitStr(String str) {
        int num = -1;
        char[] chars = str.toCharArray();
        String numStr = null;
        StringBuffer s =new StringBuffer();
        for (int i = 0; i < chars.length; i ++) {
            if (Character.isDigit(str.charAt(i))) {
                numStr = String.valueOf(str.charAt(i));
                s.append(numStr);
                num = Integer.parseInt(s.toString());
            }
        }
        return num;
    }
}
