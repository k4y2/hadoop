package Utils;

import java.util.*;

public class MapUtils {

    /**
     * Sắp xếp map theo value
     * @param map Map cần sắp xếp
     * @return Map đã được sắp xếp
     */
    public static <K extends Comparable, V extends Comparable> Map<K,V> sortByValue(Map<K,V> map) {
        List<Map.Entry<K,V>> entries = new LinkedList<>(map.entrySet());
        // Sắp xếp theo giá trị giảm dần (nhân với -1)
        Collections.sort(entries,(o1, o2) -> o1.getValue().compareTo(o2.getValue())*(-1));

        // Đưa các giá trị mới đã sort vào map mới
        Map<K,V> sortedMap = new LinkedHashMap<>();
        for (Map.Entry<K,V> entry : entries
             ) {
            K key = entry.getKey();
            V value = entry.getValue();
            sortedMap.put(key, value);
        }

        return sortedMap;
    }
}
