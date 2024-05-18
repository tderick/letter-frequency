package it.unipi.hadoop.drstrange;


import org.apache.commons.lang.StringUtils;

import java.text.Normalizer;

public class Test {
    public static void main(String[] args) {
        String line = "I AM HELLO WORLD!".toLowerCase();
        char[] tokenizer = StringUtils.deleteWhitespace(line).toCharArray();
        System.out.println(line);
        for (int i = 0; i < tokenizer.length; i++) {
//            System.out.println(tokenizer[i]);
//            System.out.println((int) tokenizer[i]);
            if(((int)tokenizer[i]>=97 && (int)tokenizer[i] <= 122)) {
                System.out.println(tokenizer[i]);
            }
        }
    }

    public static boolean isFrenchChar(int c) {
        switch (c) {
            case 128:
            case 130:
            case 131:
            case 133:
            case 135:
            case 136:
            case 137:
            case 138:
            case 139:
            case 140:
            case 144:
            case 147:
            case 150:
            case 151:
            case 153:
            case 160:
                return true;
            default:
                return false;
        }
    }
}
