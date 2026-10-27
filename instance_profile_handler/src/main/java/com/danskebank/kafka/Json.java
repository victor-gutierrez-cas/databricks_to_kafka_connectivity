package com.danskebank.kafka;

/** Minimal, dependency-free extraction of top-level string and number fields from flat JSON. */
final class Json {
    private Json() {}

    static String string(String json, String field) {
        int colon = valueStart(json, field);
        if (colon < 0) return null;
        int q1 = json.indexOf('"', colon + 1);
        if (q1 < 0) return null;
        StringBuilder sb = new StringBuilder();
        for (int i = q1 + 1; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"') return sb.toString();
            if (c != '\\' || i + 1 >= json.length()) {
                sb.append(c);
                continue;
            }
            char n = json.charAt(++i);
            switch (n) {
                case 'n': sb.append('\n'); break;
                case 'r': sb.append('\r'); break;
                case 't': sb.append('\t'); break;
                case 'b': sb.append('\b'); break;
                case 'f': sb.append('\f'); break;
                case 'u':
                    if (i + 4 < json.length()) {
                        sb.append((char) Integer.parseInt(json.substring(i + 1, i + 5), 16));
                        i += 4;
                    }
                    break;
                default: sb.append(n); // \" \\ \/
            }
        }
        return null;
    }

    static Long number(String json, String field) {
        int colon = valueStart(json, field);
        if (colon < 0) return null;
        int j = colon + 1;
        while (j < json.length() && Character.isWhitespace(json.charAt(j))) j++;
        int k = j;
        while (k < json.length() && Character.isDigit(json.charAt(k))) k++;
        return k > j ? Long.parseLong(json.substring(j, k)) : null;
    }

    private static int valueStart(String json, String field) {
        if (json == null) return -1;
        String needle = "\"" + field + "\"";
        int i = json.indexOf(needle);
        if (i < 0) return -1;
        return json.indexOf(':', i + needle.length());
    }
}
