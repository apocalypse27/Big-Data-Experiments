package top10FriendsPackage;

import java.util.Comparator;
class MyComparator implements Comparator<String> {
    public int compare(String x, String y) {
        if (x.length() == y.length()) {
            return x.compareTo(y);
        }
        
        return x.length() - y.length();
    }
}
