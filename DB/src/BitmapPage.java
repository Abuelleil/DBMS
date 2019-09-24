import javax.swing.text.html.parser.Entity;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class BitmapPage implements Serializable {
    private ArrayList<Object> values;
    private ArrayList<StringBuilder> bitmaps;
    private static int maxCapacity;
    private static boolean maxCapacitySet = false;

    public BitmapPage(ArrayList<Object> values,
                      ArrayList<StringBuilder> bitmaps) {
        this.values = values;
        this.bitmaps = bitmaps;
    }

    public BitmapPage() {
        this(new ArrayList<>(), new ArrayList<>());
    }

    public static void setMaxCapacity(int maxCapacity) {
        if (maxCapacitySet)
            System.out.println("The max capacity is already set");

        else {
            BitmapPage.maxCapacity = maxCapacity;
            maxCapacitySet = true;
        }
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public boolean isFull() {
        return values.size() >= maxCapacity;
    }

    public int size() {
        return values.size();
    }

    public static int getMaxCapacity() {
        return maxCapacity;
    }

    public void serialize(String tableName, String pageName) throws DBAppException {
        try {
            File tableFolder = new File("data/indices/" + tableName);
            if (!tableFolder.exists())
                tableFolder.mkdir();

            File file = new File("data/indices/" + tableName + "/" + pageName + ".ser");
            if (file.exists())
                file.delete();

            file.createNewFile();
            FileOutputStream fileOut = new FileOutputStream(file);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            throw new DBAppException("An error occured while serializing index page " + tableName + "/" + pageName);
        }
    }

    public static StringBuilder encode(StringBuilder bitmap) {
        StringBuilder encoding = new StringBuilder();

        if (bitmap == null || bitmap.length() == 0) return encoding;

        int length = bitmap.length(), count = 1;
        char seqcurrent = bitmap.charAt(0);

        encoding.append(seqcurrent);

        for (int i = 1; i < length; ++i) {
            char current = bitmap.charAt(i);

            if (current == seqcurrent)
                ++count;
            else {
                encoding.append(',');
                encoding.append(count);
                seqcurrent = current;
                count = 1;
            }
        }

        encoding.append(count);
        return encoding;
    }

    public static StringBuilder decode(StringBuilder encoding) {
        StringBuilder bitmap = new StringBuilder();

        if (encoding == null || encoding.length() == 0) return bitmap;

        StringTokenizer st = new StringTokenizer(encoding.toString(), ",");
        int writeCurr = Integer.parseInt(st.nextToken());

        while (st.hasMoreTokens()) {
            int count = Integer.parseInt(st.nextToken());
            while (count-- > 0)
                bitmap.append(writeCurr);
            writeCurr = ++writeCurr % 2;
        }

        return bitmap;
    }

    public static int getKeyPos(String tableName,
                                String key,
                                Object value) throws DBAppException {
        File file;
        BitmapPage page = null;
        for (int i = 0; (file = new File("data/indices/" + tableName + "/" + key + i + ".ser")).exists(); ++i) {
            try {
                FileInputStream fileIn = new FileInputStream(file);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                page = (BitmapPage) in.readObject();
                in.close();
                fileIn.close();
            } catch (Exception e) {
                throw new DBAppException("An error occured while loading index");
            }

            int index = Arrays.binarySearch(page.values.toArray(), value);
            if (index >= 0) {
                StringTokenizer st = new StringTokenizer(page.bitmaps.get(index).toString(), ",");
                if (st.nextToken().equals("1")) {
                    return 0;
                } else {
                    return Integer.parseInt(st.nextToken());
                }
            } else if (index > -page.size() - 1) {
                StringTokenizer st = new StringTokenizer(page.bitmaps.get(-index - 1).toString(), ",");
                if (st.nextToken().equals("1")) {
                    return -1;
                } else {
                    return -Integer.parseInt(st.nextToken()) - 1;
                }
            }
        }

        if (page == null)
            return -1;

        return -page.bitmaps.get(0).length() - 1;
    }

    public static void insert(String tableName,
                              Hashtable<String, Object> inserted,
                              int position) throws DBAppException {
        for (String column : inserted.keySet()) {
            File file;
            BitmapPage page = null;
            Object propVal = null, tempVal;
            StringBuilder propMap = null, tempMap;
            boolean passed = false;
            int i;
            for (i = 0; (file = new File("data/indices/" + tableName + "/" + column + i + ".ser")).exists(); ++i) {
                try {
                    FileInputStream fileIn = new FileInputStream(file);
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    page = (BitmapPage) in.readObject();
                    in.close();
                    fileIn.close();
                } catch (Exception e) {
                    throw new DBAppException("An error occured while loading index");
                }

                if (propVal != null) {
                    if (page.isFull()) {
                        tempVal = page.values.remove(page.size());
                        tempMap = page.bitmaps.remove(page.size());
                    } else {
                        tempVal = null;
                        tempMap = null;
                    }

                    page.values.add(0, propVal);
                    page.bitmaps.add(0, propMap);

                    propVal = tempVal;
                    propMap = tempMap;
                }

                for (int j = 0; j < page.size(); ++j) {
                    StringBuilder bitmap = decode(page.bitmaps.get(j));
                    Object input = inserted.get(column),
                            value = page.values.get(j);
                    int compare = ((Comparable) input).compareTo(value);
                    if (compare < 0 || passed) {
                        bitmap.insert(position, '0');
                    } else {
                        passed = true;
                        if (compare == 0) {
                            bitmap.insert(position, '1');
                        } else {
                            if (page.isFull()) {
                                propVal = page.values.remove(page.size());
                                propMap = page.bitmaps.remove(page.size());
                            }

                            int length = bitmap.length() + 1;
                            bitmap = new StringBuilder(length);
                            for (int k = 0; k < length; ++k)
                                bitmap.append(k == position ? '1' : '0');
                            page.bitmaps.add(j, encode(bitmap));
                        }
                    }

                    if (compare != 0)
                        page.bitmaps.set(j, encode(bitmap));
                }

                if (!(page.isFull() || passed)) {
                    page.values.add();
                }

                page.serialize(tableName, column + i);
            }

            if (propVal != null) {
                page = new BitmapPage();
                page.values.add(propVal);
                page.bitmaps.add(propMap);
                page.serialize(tableName, column + i);
            } else if (!passed) {

            }
        }
    }

    public static StringBuilder getExact(String tableName,
                                         String colName,
                                         Object value,
                                         boolean equals) throws DBAppException {
        File file = null;
        BitmapPage page = null;
        for (int i = 0; (file = new File("data/indices/" + tableName + "/" + colName + i + ".ser")).exists(); ++i) {
            try {
                FileInputStream fileIn = new FileInputStream(file);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                page = (BitmapPage) in.readObject();
                in.close();
                fileIn.close();
            } catch (Exception e) {
                throw new DBAppException("Error while loading index");
            }

            int index = Arrays.binarySearch(page.values.toArray(), value);

            if (index >= 0) {
                StringBuilder result = decode(page.bitmaps.get(index));
                if (equals)
                    return result;
                else
                    return NOT(result);
            } else if (index == -page.size() - 1)
                continue;
            else
                break;
        }
        char occurance = equals ? '0' : '1';
        if (page == null) {
            Page current = null;
            StringBuilder result = new StringBuilder();
            for (int i = 0; (file = new File("data/indices/" + tableName + "/" + colName + i + ".ser")).exists(); ++i) {
                try {
                    FileInputStream fileIn = new FileInputStream(file);
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
                    in.close();
                    fileIn.close();
                } catch (Exception e) {
                    throw new DBAppException("Error while loading index");
                }

                int lenght = current.size();
                for (int j = 0; j < lenght; ++j)
                    result.append(occurance);
            }

            return result;
        }

        int length = page.bitmaps.get(0).length();
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; ++i)
            result.append(occurance);
        return result;
    }

    public static StringBuilder getRange(String tableName,
                                         String colName,
                                         Object value,
                                         boolean lessThan,
                                         boolean equals) throws DBAppException {
        File file = null;
        BitmapPage page = null;
        StringBuilder builder = lessThan ? new StringBuilder() : null;
        boolean found = false;
        for (int i = 0; (file = new File("data/indices/" + tableName + "/" + colName + i + ".ser")).exists(); ++i) {
            try {
                FileInputStream fileIn = new FileInputStream(file);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                page = (BitmapPage) in.readObject();
                in.close();
                fileIn.close();
            } catch (Exception e) {
                throw new DBAppException("Error while loading index");
            }

            if (found) {
                for (StringBuilder bitmap : page.bitmaps)
                    builder = OR(builder, decode(bitmap));
                continue;
            }

            int index = Arrays.binarySearch(page.values.toArray(), value);

            if (index > -page.size() - 1) {
                if (lessThan) {
                    if (index >= 0) {
                        if (equals)
                            ++index;
                    } else
                        index = -index - 1;

                    for (int j = 0; j < index; ++j)
                        builder = OR(builder, decode(page.bitmaps.get(j)));

                    return builder;
                } else {
                    found = true;
                    if (index >= 0) {
                        if (equals)
                            builder = decode(page.bitmaps.get(index));
                    } else {
                        index = -index - 2;
                    }
                    if (builder == null)
                        builder = new StringBuilder();

                    for (int j = index + 1; j < page.size(); ++j)
                        builder = OR(builder, decode(page.bitmaps.get(j)));
                }
            } else {
                if (lessThan) {
                    for (StringBuilder bitmap : page.bitmaps)
                        builder = OR(builder, decode(bitmap));
                }
            }
        }
        if (builder != null)
            return builder;

        char occurence = '0';
        if (page == null) {
            Page current = null;
            StringBuilder result = new StringBuilder();
            for (int i = 0; (file = new File("data/indices/" + tableName + "/" + colName + i + ".ser")).exists(); ++i) {
                try {
                    FileInputStream fileIn = new FileInputStream(file);
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
                    in.close();
                    fileIn.close();
                } catch (Exception e) {
                    throw new DBAppException("Error while loading index");
                }

                int lenght = current.size();
                for (int j = 0; j < lenght; ++j)
                    result.append(occurence);
            }

            return result;
        }

        int length = page.bitmaps.get(0).length();
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; ++i)
            result.append(occurence);
        return result;
    }

    public static StringBuilder AND(StringBuilder bitmap1,
                                    StringBuilder bitmap2) throws DBAppException {
        if (bitmap1.length() != bitmap2.length())
            throw new DBAppException("The bitmap sizes are not the same");

        StringBuilder result = new StringBuilder();

        for (int i = 0; i < bitmap1.length(); ++i)
            result.append(bitmap1.charAt(i) == '0' || bitmap2.charAt(i) == '0' ? '0' : '1');

        return result;
    }

    public static StringBuilder OR(StringBuilder bitmap1,
                                   StringBuilder bitmap2) throws DBAppException {
        if (bitmap1.length() != bitmap2.length())
            throw new DBAppException("The bitmap sizes are not the same");

        StringBuilder result = new StringBuilder();

        for (int i = 0; i < bitmap1.length(); ++i)
            result.append(bitmap1.charAt(i) == '0' && bitmap2.charAt(i) == '0' ? '0' : '1');

        return result;
    }

    public static StringBuilder XOR(StringBuilder bitmap1,
                                    StringBuilder bitmap2) throws DBAppException {
        if (bitmap1.length() != bitmap2.length())
            throw new DBAppException("The bitmap sizes are not the same");

        StringBuilder result = new StringBuilder();

        for (int i = 0; i < bitmap1.length(); ++i)
            result.append(bitmap1.charAt(i) == bitmap2.charAt(i) ? '0' : '1');

        return result;
    }

    public static StringBuilder NOT(StringBuilder bitmap) {
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < bitmap.length(); ++i)
            result.append(bitmap.charAt(i) == '1' ? '0' : '1');

        return result;
    }
}
//    public static class BitmapEntry implements Comparable<BitmapEntry>
//    {
//        public Class<?> type;
//        public Object value;
//        public StringBuilder bitmap;
//
//        public BitmapEntry(Object value, StringBuilder bitmap)
//        {
//            this.value = value;
//            this.bitmap = bitmap;
//        }
//
//        public BitmapEntry()
//        {
//            this(null, new StringBuilder());
//        }
//
//        @Override
//        public int compareTo(BitmapEntry o)
//        {
//            try { return (Integer) type.getMethod("compareTo", type).invoke(value, o.value); }
//            catch (Exception e) { e.printStackTrace(); return 0; }
//        }
//    }

