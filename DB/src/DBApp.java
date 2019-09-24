import java.io.*;
import java.util.*;

public class DBApp
{
    private HashSet<String> availableTypes, availableOpInside, availableOpOutside;

    public void init()
    {
        Properties prop = new Properties();
        InputStream input = null;

        try
        {
            input = new FileInputStream("config/DBApp.config");

            prop.load(input);

            Page.setMaxCapacity(Integer.parseInt(prop.getProperty("MaximumRowsCountinPage")));
            BitmapPage.setMaxCapacity(Integer.parseInt(prop.getProperty("BitmapSize")));

        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            if (input != null)
            {
                try
                {
                    input.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

        availableTypes = new HashSet<String>();
        availableTypes.add("java.lang.Integer");
        availableTypes.add("java.lang.String");
        availableTypes.add("java.lang.Double");
        availableTypes.add("java.lang.Boolean");
        availableTypes.add("java.util.Date");

        availableOpInside = new HashSet<String>();
        availableOpInside.add(">");
        availableOpInside.add(">=");
        availableOpInside.add("<");
        availableOpInside.add("<=");
        availableOpInside.add("!=");
        availableOpInside.add("=");

        availableOpOutside = new HashSet<String>();
        availableOpOutside.add("AND");
        availableOpOutside.add("OR");
        availableOpOutside.add("XOR");
    }

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType ) throws DBAppException
    {
        if (strTableName.equals(""))
            throw new DBAppException("The table name can not be empty");

        for (String columnName : htblColNameType.keySet())
            if (columnName.equals(""))
                throw new DBAppException("The column name can not be empty");

        if (!htblColNameType.containsKey(strClusteringKeyColumn))
            throw new DBAppException("The clustering key has to be one of the table's columns");

        final String COMMA_DELIMITER = ",";
        final String NEW_LINE_SEPARATOR = "\n";
        BufferedReader fileReader =null;
        boolean TableFound=false;
        String line;

        try
        {
            fileReader = new BufferedReader(new FileReader("data/metadata.csv"));
            fileReader.readLine();
            while ((line=fileReader.readLine())!=null) {
                String[] tokens=line.split(",");
                if(tokens[0].equals(strTableName))
                    TableFound=true;
            }
        }
        catch (IOException io)
        {
            throw new DBAppException("Something went wrong with csv file");
        }
        if(TableFound)
            throw  new DBAppException("");


        for (String ColName : htblColNameType.keySet()) {
            if(!(availableTypes.contains(htblColNameType.get(ColName))))
                throw  new DBAppException("Eltype da ghalat yabny");
        }


        FileWriter fileWriter = null;

        try {
            fileWriter = new FileWriter(new File("data/metadata.csv"));
            //Write a new student object list to the CSV file
            for (String ColName : htblColNameType.keySet()) {
                fileWriter.append(strTableName);
                fileWriter.append(COMMA_DELIMITER);

                fileWriter.append(ColName);
                fileWriter.append(COMMA_DELIMITER);

                fileWriter.append(htblColNameType.get(ColName));
                fileWriter.append(COMMA_DELIMITER);

                fileWriter.append(ColName.equals(strClusteringKeyColumn)?"True":"False");
                fileWriter.append(COMMA_DELIMITER);

                fileWriter.append("False");

                fileWriter.append(NEW_LINE_SEPARATOR);
            }
            System.out.println("CSV file was created successfully !!!");

        } catch (Exception e) {
            System.out.println("Error in CsvFileWriter !!!");
            e.printStackTrace();
        } finally {

            try {
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                System.out.println("Error while flushing/closing fileWriter !!!");
                e.printStackTrace();
            }
        }
    }

    public void createBitmapIndex(String strTableName,
                                  String strColName) throws DBAppException
    {
        File file = new File("data/metadata.csv");
        BufferedReader fileReader = null;
        Queue<String[]> history = new LinkedList<>();
        boolean tableFound = false, columnFound = false;
        Class<?> cls = null;

        try
        {
            String line;
            fileReader = new BufferedReader(new FileReader(file));

            history.add(fileReader.readLine().split(","));

            while ((line = fileReader.readLine()) != null)
            {
                String[] tokens = line.split(",");

                if (tokens.length == 5)
                {
                    if (tokens[0].equals(strTableName))
                    {
                        tableFound = true;
                        if (tokens[1].equals(strColName))
                        {
                            columnFound = true;
                            cls = Class.forName(tokens[2]);
                            if (tokens[4].equals("True"))
                                throw new DBAppException("This column is already indexed");
                            else
                                tokens[4] = "True";
                        }
                    }
                }
            }

            if (!tableFound)
                throw new DBAppException("There's no table with that name");

            if (!columnFound)
                throw new DBAppException("There's no column with that name in this table");
        }
        catch (Exception e) { e.printStackTrace(); }
        finally
        {
            try { fileReader.close(); }
            catch (IOException e) { e.printStackTrace(); }
        }

        FileWriter fileWriter = null;

        try
        {
            file.delete();
            file.createNewFile();

            fileWriter = new FileWriter(file);

            while (!history.isEmpty())
            {
                String[] tokens = history.poll();
                for (int i = 0; i < tokens.length; ++i)
                    fileWriter.append(tokens[i] + (i < tokens.length - 1 ? "," : "\n"));
            }
        }
        catch (Exception e)
        {
            System.out.println("Error in CsvFileWriter !!!");
            e.printStackTrace();
        }
        finally
        {

            try
            {
                fileWriter.flush();
                fileWriter.close();
            }
            catch (IOException e)
            {
                System.out.println("Error while flushing/closing fileWriter !!!");
                e.printStackTrace();
            }
        }

        try
        {
            ArrayList<Object> values = new ArrayList<>();
            ArrayList<String> bitmaps = new ArrayList<>();
            Object[] serchSpace = values.toArray();
            String allZeros = "";

            for (int i = 0; (file = new File("data/" + strTableName + i + ".ser")).exists(); ++i)
            {
                FileInputStream fileIn = new FileInputStream(file);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                Page page = new Page((Vector<Hashtable<String, Object>>) in.readObject());
                in.close();
                fileIn.close();

                for (int j = 0; j < page.size(); ++j)
                {
                    allZeros += "0";
                    Hashtable<String, Object> record = page.get(j);
                    Object value = record.get(strColName);
                    int index = Arrays.binarySearch(serchSpace, value);

                    if (index < 0)
                    {
                        index = -index - 1;
                        values.add(index, value);
                        bitmaps.add(index, allZeros);
                    }
                    else
                    {
                        bitmaps.set(index, bitmaps.get(index) + "1");
                    }

                    for (int k = 0; k < values.size(); ++k)
                    {
                        if (k != index)
                        {
                            bitmaps.set(k, bitmaps.get(k) + "0");
                        }
                    }
                }
            }

            ArrayList<StringBuilder> encodings = new ArrayList<>();
            for (String bitmap : bitmaps)
                encodings.add(BitmapPage.encode(new StringBuilder(bitmap)));

            int index = 0, page = 0;
            while (index < values.size())
            {
                int border = Math.max(index + BitmapPage.getMaxCapacity(), values.size());
                ArrayList<Object> insertVals = new ArrayList<>();
                ArrayList<StringBuilder> insertBitmaps = new ArrayList<>();

                for (int i = index; i < border; ++i)
                {
                    insertVals.add(values.get(i));
                    insertBitmaps.add(encodings.get(i));
                }

                new BitmapPage(insertVals, insertBitmaps).serialize(strTableName, strColName + page++);
                index = border;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new DBAppException("Error while making index");
        }
    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException
    {
        TableTuple tuple = getColumns(strTableName);
        Hashtable<String, String> columns = tuple.columns;
        String key = tuple.key;
        HashSet<String> indexedColumns = tuple.indexedColumns;
        if (htblColNameValue.get(key) == null)
            throw new DBAppException("The clustering key does not have a value");
        htblColNameValue = validateColumns(columns, htblColNameValue);

        if (indexedColumns.contains(key))
        {
            int pos = BitmapPage.getKeyPos(strTableName, key, htblColNameValue.get(key));
            if (pos >= 0)
                throw new DBAppException("There is already a record with the same value for clustering key");

            int maxCapacity = Page.getMaxCapacity(),
                    page = pos / maxCapacity,
                    index = pos % maxCapacity,
                    j;

            Page previous, current;
            File file = new File("data/" + strTableName + page + ".ser");
            try
            {
                FileInputStream fileIn = new FileInputStream(file);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
                in.close();
                fileIn.close();
            }
            catch (Exception e)
            {
                throw new DBAppException("An error occured while accessing the table pages");
            }

            Hashtable<String, Object> propagate = current.removeLast(), temp;
            current.add(index, htblColNameValue);
            current.serialize(strTableName + page);

            for (j = page + 1; (file = new File("data/" + strTableName + j + ".ser")).exists(); ++j)
            {
                try
                {
                    FileInputStream fileIn = new FileInputStream(file);
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
                    in.close();
                    fileIn.close();
                }
                catch (Exception e)
                {
                    throw new DBAppException("An error occured while accessing the table pages");
                }

                if (current.isFull())
                {
                    temp = current.removeLast();
                    current.addFirst(propagate);
                    current.serialize(strTableName + j);
                    propagate = temp;
                }
                else
                {
                    current.addFirst(propagate);
                    current.serialize(strTableName + j);
                    propagate = null;
                    break;
                }
            }

            //shifted all records but last one have no page to be put in
            if (propagate != null)
            {
                current = new Page();
                current.addFirst(propagate);
                current.serialize(strTableName + j);
            }
        }

        Comparator<Hashtable<String, Object>> comparator = null;
        try
        {
            Class<?> cls = Class.forName(columns.get(key));
            comparator =
                    (a, b) -> {
                        try {
                            return (Integer) cls.getMethod("compareTo", cls).invoke(a.get(key), b.get(key));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return 0;
                    };
        }
        catch (Exception e)
        {
            throw new DBAppException("There is already a record with the same value for clustering key");
        }

        Page current = null;
        int i = 0, index = 0;
        try
        {
            for (i = 0; true; ++i)
            {
                File file = new File("data/" + strTableName + i + ".ser");

                // reached last page and still hasn't reached insertion point
                if (!file.exists())
                {
                    file.createNewFile();
                    Page produced = new Page();
                    produced.addFirst(htblColNameValue);
                    produced.serialize(strTableName + i);
                    break;
                }

                FileInputStream fileIn = new FileInputStream(file);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
                in.close();
                fileIn.close();
                index = - Arrays.binarySearch(current.toArray(), htblColNameValue, comparator) - 1;
                if (index < 0)
                    throw new DBAppException("");

                if (index == current.size())
                {
                    //may be inserted in a coming page
                    if (current.isFull())
                        continue;

                        //this is the last page and it's not full yet and insertion is in the end
                    else {
                        current.addLast(htblColNameValue);
                        current.serialize(strTableName + i);
                    }
                }

                //this is the page with insertion point but it's full and records need
                //to be shifted through pages
                else if (current.isFull())
                {
                    Hashtable<String, Object> propagate = current.removeLast(), temp;
                    current.add(index, htblColNameValue);
                    current.serialize(strTableName + i);
                    int j;

                    for (j = i + 1; (file = new File("data/" + strTableName + j + ".ser")).exists(); ++j)
                    {
                        fileIn = new FileInputStream(file);
                        in = new ObjectInputStream(fileIn);
                        current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
                        in.close();
                        fileIn.close();

                        if (current.isFull())
                        {
                            temp = current.removeLast();
                            current.addFirst(propagate);
                            current.serialize(strTableName + j);
                            propagate = temp;
                        }
                        else
                        {
                            current.addFirst(propagate);
                            current.serialize(strTableName + j);
                            propagate = null;
                            break;
                        }
                    }

                    //shifted all records but last one have no page to be put in
                    if (propagate != null)
                    {
                        current = new Page();
                        current.addFirst(propagate);
                        current.serialize(strTableName + j);
                    }
                }

                //this is last page and is not full yet and insertion is in the middle
                else
                {
                    current.add(index, htblColNameValue);
                    current.serialize(strTableName + i);
                }
                break;
            }
        }
        catch (Exception e)
        {
            throw new DBAppException(e.getClass().getName() + ": " + e.getMessage());
        }

        Hashtable<String, Object> insertIntoIndex = new Hashtable<>();
        for (String column : indexedColumns)
        {
            Object value = htblColNameValue.get(column);
            if (value != null)
                insertIntoIndex.put(column, value);
        }

        if (!insertIntoIndex.isEmpty())
            BitmapPage.insert(strTableName, insertIntoIndex, i * Page.getMaxCapacity() + index);
    }

    public void updateTable(String strTableName, Object keyVal, Hashtable<String,Object> htblColNameValue ) throws DBAppException {
        TableTuple pair = getColumns(strTableName);
        Hashtable<String, String> columns = pair.columns;
        String key = pair.key;
        validateColumns(columns, htblColNameValue);
        try { Class.forName(columns.get(key)).cast(keyVal);}
        catch (ClassNotFoundException nf)
        {
            throw new DBAppException("The type of the key is not recognized");
        }
        catch (ClassCastException cc)
        {
            throw new DBAppException("The keyVal type is not compatible with the stored type of the key");
        }

        Comparator<Hashtable<String, Object>> comparator = null;
        try
        {
            Class<?> cls = Class.forName(columns.get(key));
            comparator =
                    (a, b) -> {
                        try {
                            return (Integer) cls.getMethod("compareTo", cls).invoke(a.get(key), b.get(key));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return 0;
                    };
        }
        catch (Exception e)
        {
            throw new DBAppException("");
        }

        Page current = null;
        try {
            for (int i = 0; true; ++i) {
                if(indexedColumns.contains(key))
                File file = new File("data/" + strTableName + i + ".ser");
                // reached last page and still hasn't reached insertion point
                if (!file.exists()) {
                    System.out.println("No record to update");
                    return;
                }
                FileInputStream fileIn = new FileInputStream(file);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
                in.close();
                fileIn.close();
                int index = Arrays.binarySearch(current.toArray(), htblColNameValue, comparator);
                if(index>=0){
                    Hashtable <String,Object> record = current.get(index);
                    deleteFromTable(strTableName,record);
                    for(String column:htblColNameValue.keySet()){
                        record.put(column, htblColNameValue.get(column));
                    }
                        insertIntoTable(strTableName,record);
                    System.out.println("Record updated successfully");
                    return;
                }else{
                    if(index==-current.size()-1){
                        continue;
                    }else{
                        System.out.println("error 404: record not found");
                        return;
                    }
                }

            }
        } catch (Exception e) {
            throw new DBAppException(e.getClass().getName() + ": " + e.getMessage());
        }
    }

//    public void deleteFromTable(String strTableName,
//                                Hashtable<String,Object> htblColNameValue) throws DBAppException
//    {
//        TableTuple tuple = getColumns(strTableName);
//        Hashtable<String, String> columns = tuple.columns;
//        String key = tuple.key;
//        if (htblColNameValue.get(key) == null)
//            throw new DBAppException("The clustering key does not have a value");
//        htblColNameValue = validateColumns(columns, htblColNameValue);
//
//        Comparator<Hashtable<String, Object>> comparator = null;
//        try
//        {
//            Class<?> cls = Class.forName(columns.get(key));
//            comparator =
//                    (a, b) -> {
//                        try {
//                            return (Integer) cls.getMethod("compareTo", cls).invoke(a.get(key), b.get(key));
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        return 0;
//                    };
//        }
//        catch (Exception e)
//        {
//            throw new DBAppException("");
//        }
//
//        Page previous, current = null;
//        try
//        {
//            for (int i = 0; true; ++i)
//            {
//                File file = new File("data/" + strTableName + i + ".ser");
//
//                // reached last page and still hasn't reached deletion point
//                /*
//                if (!file.exists())
//                {
//                    file.createNewFile();
//                    Page produced = new Page();
//                    produced.addFirst(htblColNameValue);
//                    produced.serialize(strTableName + i);
//                    break;
//                }
//				*/
//
//                FileInputStream fileIn = new FileInputStream(file);
//                ObjectInputStream in = new ObjectInputStream(fileIn);
//                current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
//                in.close();
//                fileIn.close();
//                int index = Arrays.binarySearch(current.toArray(), htblColNameValue, comparator);
//                index = index < 0 ? - index - 1 : index;
//
//                if (index == current.size())
//                {
//                    //may be deleted in a coming page
//                    if (current.isFull())
//                        continue;
//
//                        //this is the last page and it's not full yet and deletion is in the index
//                    else {
//                        current.remove(index);
//                        current.serialize(strTableName + i);
//                    }
//                }
//
//                //this is the page with insertion point but it's full and records need
//                //to be shifted through pages
//                else if (current.isFull())
//                {
//                    previous = current;
//                    fileIn = new FileInputStream(file = new File("data/" + strTableName + i+1 + ".ser"));
//
//                    in = new ObjectInputStream(fileIn);
//                    current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
//                    in.close();
//                    fileIn.close();
//
//                    Hashtable<String, Object> propagate = current.removeFirst(), temp;
//                    current.serialize(strTableName + i+1);
//
//                    previous.addFirst(propagate);
//                    previous.serialize(strTableName + i);
//
//                    int j;
//
//                    for (j = i + 2; (file = new File("data/" + strTableName + j + ".ser")).exists(); ++j)
//                    {
//                        previous = current;
//
//                        fileIn = new FileInputStream(file);
//                        in = new ObjectInputStream(fileIn);
//                        current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
//                        in.close();
//                        fileIn.close();
//
//                        if (!(current.isEmpty()))
//                        {
//                            temp = current.removeFirst();
//                            current.serialize(strTableName + j);
//                            propagate = temp;
//                            previous.addFirst(propagate);
//                            previous.serialize(strTableName + (j-1));
//                        }
//                        /*
//                        else
//                        {
//                        	temp = current.removeFirst();
//                            current.serialize(strTableName + j);
//                            propagate = temp;
//                            previous.addFirst(propagate);
//                            previous.serialize(strTableName + (j-1));
//
//                            current.addFirst(propagate);
//                            current.serialize(strTableName + j);
//                            propagate = null;
//                            break;
//                        }
//                        */
//                    }
//
//                    /*
//                    //shifted all records but last one have no page to be put in
//                    if (propagate != null)
//                    {
//                        current = new Page();
//                        current.addFirst(propagate);
//                        current.serialize(strTableName + j);
//                    }
//                    */
//                }
//
//
//                //this is last page and is not full yet and insertion is in the middle
//                /*
//                else
//                {
//                    current.add(index, htblColNameValue);
//                    current.serialize(strTableName + i);
//                }
//                break;
//                */
//            }
//        }
//        catch (Exception e)
//        {
//            throw new DBAppException(e.getClass().getName() + ": " + e.getMessage());
//        }
//    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException
    {
        TableTuple tuple = getColumns(strTableName);
        validateColumns(tuple.columns, htblColNameValue);

        File file;
        Page previous, current = null;
        int i;
        for (i = 0; (file = new File("data/" + strTableName + i + ".ser")).exists(); ++i)
        {
            previous = current;
            try
            {
                FileInputStream fileIn = new FileInputStream(file);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
                in.close();
                fileIn.close();
            }
            catch (Exception e)
            {
                throw new DBAppException("Error while loading index");
            }

            int preSize = current.size();
            for (int j = 0; j < current.size(); ++j)
            {
                Hashtable<String, Object> record = current.get(j);
                boolean match = true;

                for (String column : htblColNameValue.keySet())
                    match &= record.get(column).equals(htblColNameValue.get(column));

                if (match)
                    current.remove(j--);
            }

            if (previous != null)
            {
                while (!(previous.isFull() || current.isEmpty()))
                    previous.addLast(current.removeFirst());

                if (!current.isEmpty())
                {
                    previous.serialize(strTableName + (i - 1));
                }
            }

            if (current.isEmpty())
            {
                file.delete();
                for (int j = i + 1; (file = new File("data/" + strTableName + j + ".ser")).exists(); ++j)
                    file.renameTo(new File("data/" + strTableName + (j - 1) + ".ser"));

                current = previous;
                --i;
            }
        }

        if (current != null)
            current.serialize(strTableName + (i - 1));
    }

    public ArrayList selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException, IOException, ClassNotFoundException {
        TableTuple tuple = validateSQLQuery(arrSQLTerms);
        Hashtable<String,Integer> iterate = new Hashtable<>();
        ArrayList<StringBuilder>  bitmaps = new ArrayList<>();
        ArrayList<Hashtable<String,Object>>result= new ArrayList();
//        Iterator<Hashtable<String,Object>> result = new Iterator<Hashtable<String, Object>>() {
//            @Override
//            public boolean hasNext() {
//                return false;
//            }
//
//            @Override
//            public Hashtable<String, Object> next() {
//                return null;
//            }
//        };
        for(int i=0;i<strarrOperators.length;i++){
            if(tuple.indexedColumns.contains(arrSQLTerms[i]._strTableName)){
                switch(arrSQLTerms[i]._strOperator){
                    case "==": bitmaps.add(BitmapPage.getExact(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,true));break;
                    case "!=": bitmaps.add(BitmapPage.getExact(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,false));break;
                    case ">": bitmaps.add(BitmapPage.getRange(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,false,false));break;
                    case ">=": bitmaps.add(BitmapPage.getRange(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,false,true)); break;
                    case "<": bitmaps.add(BitmapPage.getRange(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,true,false)); break;
                    case "<=":bitmaps.add(BitmapPage.getRange(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,true,true)); break;
                }
            }else{
                iterate.add(arrSQLTerms[i]._strColumnName,i);
                bitmaps.add(new StringBuilder());
            }
        }
        String tableName = arrSQLTerms[0]._strTableName;
        File file = null;
        for(int i=0;(file=new File("data/" + tableName + i + ".ser")).exists();++i)
        {
            FileInputStream fileIn = new FileInputStream(file);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Page page = new Page((Vector<Hashtable<String, Object>>) in.readObject());
            for(int j=0;j<=page.size();j++){
                for(int k=0;k<=iterate.size();k++){
                    if(page.get(j).equals(arrSQLTerms[k]._strColumnName)){
                        bitmaps.get(i).append(1);
                    }else{
                        bitmaps.get(i).append(0);
                    }
                }
            }
            in.close();
            fileIn.close();
        }
        ArrayList<String> bitwiseOperators = new ArrayList<>(strarrOperators.length);
        for(String op : strarrOperators){
            bitwiseOperators.add(op);
        }

        for(int i=0;i<bitwiseOperators.size();i++){
            if(bitwiseOperators.get(i).equals("AND")){
                bitmaps.set(i, BitmapPage.AND(bitmaps.get(i), bitmaps.get(i+1)));
                bitmaps.remove(i+1);
                bitwiseOperators.remove(i--);
            }
        }

        for(int i=0;i<bitwiseOperators.size();i++){
            if(bitwiseOperators.get(i).equals("OR")){
                bitmaps.set(i, BitmapPage.OR(bitmaps.get(i), bitmaps.get(i+1)));
                bitmaps.remove(i+1);
                bitwiseOperators.remove(i--);
            }
        }

        for(int i=0;i<bitwiseOperators.size();i++) {
            if (bitwiseOperators.get(i).equals("XOR")) {
                bitmaps.set(i, BitmapPage.XOR(bitmaps.get(i), bitmaps.get(i + 1)));
                bitmaps.remove(i + 1);
                bitwiseOperators.remove(i--);
            }
        }
        for(int i=0;(file=new File("data/" + tableName + i + ".ser")).exists();++i)
        {
            FileInputStream fileIn = new FileInputStream(file);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Page page = new Page((Vector<Hashtable<String, Object>>) in.readObject());
            for(int j=0;j<page.size();j++){
                    if (bitmaps.get(0).charAt(j) == 1) {
                        result.add(page.get(j));
                    }
            }
            in.close();
            fileIn.close();
        }

        return result;
    }

    public TableTuple validateSQLQuery(SQLTerm[] arrSQLTerms,
                                       String[] strarrOperators) throws DBAppException
    {
        if (arrSQLTerms.length != strarrOperators.length + 1)
            throw new DBAppException("The number of SQL terms and operators are not compatible");

        String tableName = "";
        for (SQLTerm term : arrSQLTerms)
        {
            if (term._strTableName.equals(""))
                throw new DBAppException("Invalid table name");

            if (tableName.equals(""))
                tableName = term._strTableName;
            else if (!tableName.equals(term._strTableName))
                throw new DBAppException("The 'SELECT' query does not support joins");

            if (!availableOpInside.contains(term._strOperator))
                throw new DBAppException("The " + term._strOperator + " operator is not available");

            if (term._objValue == null && !(term._strOperator.equals("==") || term._strOperator.equals("!=")))
                throw new DBAppException("A null value can not be in a range query");
        }

        for (String operator : strarrOperators)
            if (!availableOpOutside.contains(operator))
                throw new DBAppException("The " + operator + " operator is not available");

        TableTuple tuple = getColumns(tableName);
        Hashtable<String, String> columns = tuple.columns;
        for (SQLTerm term : arrSQLTerms)
        {
            if (!columns.containsKey(term._strColumnName))
                throw new DBAppException("Column " + term._strColumnName + " does not exist in table " + tableName);

            if (term._objValue != null)
                try
                {
                    Class.forName(columns.get(term._strColumnName)).cast(term._objValue);
                }
                catch (ClassNotFoundException nf)
                {
                    throw new DBAppException("The type of the key is not recognized");
                }
                catch (ClassCastException cc)
                {
                    throw new DBAppException("The keyVal type is not compatible with the stored type of the key");
                }
        }

        return tuple;
    }

    public TableTuple getColumns(String tableName) throws DBAppException
    {
        Hashtable<String, String> columns = new Hashtable<>();
        String key = "";
        HashSet<String> indexedColumns = new HashSet<>();

        BufferedReader fileReader = null;

        try
        {
            String line;
            boolean keySet = false;
            fileReader = new BufferedReader(new FileReader("data/metadata.csv"));

            fileReader.readLine();

            while ((line = fileReader.readLine()) != null)
            {
                String[] tokens = line.split(",");

                if (tokens.length == 5 )
                    if (tokens[0].equals(tableName))
                    {
                        columns.put(tokens[1], tokens[2]);
                        if (tokens[3].equals("True"))
                            if (keySet)
                                throw new DBAppException("No multi keys are allowed");
                            else
                            {
                                key = tokens[1];
                                keySet = true;
                            }

                        if (tokens[4].equals("True"))
                            indexedColumns.add(tokens[1]);
                    }
                    else
                        break;
            }

            if (columns.isEmpty())
                throw new DBAppException("There's no table with that name");

            if (!keySet)
                throw new DBAppException("There's no key column");
        }
        catch (DBAppException dbe) { throw dbe; }
        catch (Exception e) { e.printStackTrace(); }
        finally
        {
            try { fileReader.close(); }
            catch (IOException e) { e.printStackTrace(); }
        }

        return new TableTuple(columns, key, indexedColumns);
    }

    public Hashtable<String, Object> validateColumns(Hashtable<String, String> tamplet,
                                                     Hashtable<String, Object> givin)
            throws DBAppException
    {
        if (tamplet.size() < givin.size())
            throw new DBAppException("The cardinality of the input does not mach the table's");

        for (String column : givin.keySet())
        {
            if (!tamplet.containsKey(column))
                throw new DBAppException("The input row contains invalid column " + column);

            Object value = givin.get(column);
            if (value != null)
            {

                String tampletClass = tamplet.get(column),
                        givinClass = value.getClass().getCanonicalName();
                if (!tampletClass.equals(givinClass))
                    throw new DBAppException("The column " + column + " should have been " + tampletClass + ", but it was " + givinClass);
            }
        }

        Hashtable<String, Object> result = (Hashtable<String, Object>) givin.clone();

        for (String column : tamplet.keySet())
            if (!result.containsKey(column))
                result.put(column, null);

        return result;
    }

    public static class TableTuple
    {
        public Hashtable<String, String> columns;
        public String key;
        public HashSet<String> indexedColumns;

        public TableTuple(Hashtable<String, String> columns,
                          String key,
                          HashSet<String> indexedColumns)
        {
            this.columns = columns;
            this.key = key;
            this.indexedColumns = indexedColumns;
        }
    }

    public static class IndexPair
    {
        public int page;
        public int index;
        public boolean present;

        public IndexPair(int page, int index, boolean present)
        {
            this.page = page;
            this.index = index;
            this.present = present;
        }
    }
}

//import java.io.*;
//import java.util.*;
//
//public class DBApp
//{
//    private HashSet<String> availableTypes, availableOpInside, availableOpOutside;
//
//    public void init()
//    {
//        Properties prop = new Properties();
//        InputStream input = null;
//
//        try
//        {
//            input = new FileInputStream("config/DBApp.config");
//
//            prop.load(input);
//
//            Page.setMaxCapacity(Integer.parseInt(prop.getProperty("MaximumRowsCountinPage")));
//            BitmapPage.setMaxCapacity(Integer.parseInt(prop.getProperty("BitmapSize")));
//
//        }
//        catch (IOException ex)
//        {
//            ex.printStackTrace();
//        }
//        finally
//        {
//            if (input != null)
//            {
//                try
//                {
//                    input.close();
//                }
//                catch (IOException e)
//                {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        availableTypes = new HashSet<String>();
//        availableTypes.add("java.lang.Integer");
//        availableTypes.add("java.lang.String");
//        availableTypes.add("java.lang.Double");
//        availableTypes.add("java.lang.Boolean");
//        availableTypes.add("java.util.Date");
//
//        availableOpInside = new HashSet<String>();
//        availableOpInside.add(">");
//        availableOpInside.add(">=");
//        availableOpInside.add("<");
//        availableOpInside.add("<=");
//        availableOpInside.add("!=");
//        availableOpInside.add("=");
//
//        availableOpOutside = new HashSet<String>();
//        availableOpOutside.add("AND");
//        availableOpOutside.add("OR");
//        availableOpOutside.add("XOR");
//    }
//
//    public void createTable(String strTableName,
//                            String strClusteringKeyColumn,
//                            Hashtable<String,String> htblColNameType ) throws DBAppException
//    {
//        final String COMMA_DELIMITER = ",";
//        final String NEW_LINE_SEPARATOR = "\n";
//        BufferedReader fileReader =null;
//        boolean TableFound=false;
//        String line;
//
//        try
//        {
//            fileReader = new BufferedReader(new FileReader("data/metadata.csv"));
//            fileReader.readLine();
//            while ((line=fileReader.readLine())!=null) {
//                String[] tokens=line.split(",");
//                if(tokens[0].equals(strTableName))
//                    TableFound=true;
//            }
//        }
//        catch (IOException io)
//        {
//            throw new DBAppException("Something went wrong with csv file");
//        }
//        if(TableFound)
//            throw  new DBAppException("");
//
//
//        for (String ColName : htblColNameType.keySet()) {
//            if(!(availableTypes.contains(htblColNameType.get(ColName))))
//                throw  new DBAppException("Eltype da ghalat yabny");
//        }
//
//
//        FileWriter fileWriter = null;
//
//        try {
//            fileWriter = new FileWriter(new File("data/metadata.csv"));
//            //Write a new student object list to the CSV file
//            for (String ColName : htblColNameType.keySet()) {
//                fileWriter.append(strTableName);
//                fileWriter.append(COMMA_DELIMITER);
//
//                fileWriter.append(ColName);
//                fileWriter.append(COMMA_DELIMITER);
//
//                fileWriter.append(htblColNameType.get(ColName));
//                fileWriter.append(COMMA_DELIMITER);
//
//                fileWriter.append(ColName.equals(strClusteringKeyColumn)?"True":"False");
//                fileWriter.append(COMMA_DELIMITER);
//
//                fileWriter.append("False");
//
//                fileWriter.append(NEW_LINE_SEPARATOR);
//            }
//            System.out.println("CSV file was created successfully !!!");
//
//        } catch (Exception e) {
//            System.out.println("Error in CsvFileWriter !!!");
//            e.printStackTrace();
//        } finally {
//
//            try {
//                fileWriter.flush();
//                fileWriter.close();
//            } catch (IOException e) {
//                System.out.println("Error while flushing/closing fileWriter !!!");
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public void createBitmapIndex(String strTableName,
//                                  String strColName) throws DBAppException
//    {
//        File file = new File("data/metadata.csv");
//        BufferedReader fileReader = null;
//        Queue<String[]> history = new LinkedList<>();
//        boolean tableFound = false, columnFound = false;
//        Class<?> cls = null;
//
//        try
//        {
//            String line;
//            fileReader = new BufferedReader(new FileReader(file));
//
//            history.add(fileReader.readLine().split(","));
//
//            while ((line = fileReader.readLine()) != null)
//            {
//                String[] tokens = line.split(",");
//
//                if (tokens.length == 5)
//                {
//                    if (tokens[0].equals(strTableName))
//                    {
//                        tableFound = true;
//                        if (tokens[1].equals(strColName))
//                        {
//                            columnFound = true;
//                            cls = Class.forName(tokens[2]);
//                            if (tokens[4].equals("True"))
//                                throw new DBAppException("This column is already indexed");
//                            else
//                                tokens[4] = "True";
//                        }
//                    }
//                }
//            }
//
//            if (!tableFound)
//                throw new DBAppException("There's no table with that name");
//
//            if (!columnFound)
//                throw new DBAppException("There's no column with that name in this table");
//        }
//        catch (Exception e) { e.printStackTrace(); }
//        finally
//        {
//            try { fileReader.close(); }
//            catch (IOException e) { e.printStackTrace(); }
//        }
//
//        FileWriter fileWriter = null;
//
//        try
//        {
//            file.delete();
//            file.createNewFile();
//
//            fileWriter = new FileWriter(file);
//
//            while (!history.isEmpty())
//            {
//                String[] tokens = history.poll();
//                for (int i = 0; i < tokens.length; ++i)
//                    fileWriter.append(tokens[i] + (i < tokens.length - 1 ? "," : "\n"));
//            }
//        }
//        catch (Exception e)
//        {
//            System.out.println("Error in CsvFileWriter !!!");
//            e.printStackTrace();
//        }
//        finally
//        {
//
//            try
//            {
//                fileWriter.flush();
//                fileWriter.close();
//            }
//            catch (IOException e)
//            {
//                System.out.println("Error while flushing/closing fileWriter !!!");
//                e.printStackTrace();
//            }
//        }
//
//        try
//        {
//            ArrayList<Object> values = new ArrayList<>();
//            ArrayList<String> bitmaps = new ArrayList<>();
//            Object[] serchSpace = values.toArray();
//            String allZeros = "";
//
//            for (int i = 0; (file = new File("data/" + strTableName + i + ".ser")).exists(); ++i)
//            {
//                FileInputStream fileIn = new FileInputStream(file);
//                ObjectInputStream in = new ObjectInputStream(fileIn);
//                Page page = new Page((Vector<Hashtable<String, Object>>) in.readObject());
//                in.close();
//                fileIn.close();
//
//                for (int j = 0; j < page.size(); ++j)
//                {
//                    allZeros += "0";
//                    Hashtable<String, Object> record = page.get(j);
//                    Object value = record.get(strColName);
//                    int index = Arrays.binarySearch(serchSpace, value);
//
//                    if (index < 0)
//                    {
//                        index = -index - 1;
//                        values.add(index, value);
//                        bitmaps.add(index, allZeros);
//                    }
//                    else
//                    {
//                        bitmaps.set(index, bitmaps.get(index) + "1");
//                    }
//
//                    for (int k = 0; k < values.size(); ++k)
//                    {
//                        if (k != index)
//                        {
//                            bitmaps.set(k, bitmaps.get(k) + "0");
//                        }
//                    }
//                }
//            }
//
//            ArrayList<StringBuilder> encodings = new ArrayList<>();
//            for (String bitmap : bitmaps)
//                encodings.add(BitmapPage.encode(new StringBuilder(bitmap)));
//
//            int index = 0, page = 0;
//            while (index < values.size())
//            {
//                int border = Math.max(index + BitmapPage.getMaxCapacity(), values.size());
//                ArrayList<Object> insertVals = new ArrayList<>();
//                ArrayList<StringBuilder> insertBitmaps = new ArrayList<>();
//
//                for (int i = index; i < border; ++i)
//                {
//                    insertVals.add(values.get(i));
//                    insertBitmaps.add(encodings.get(i));
//                }
//
//                new BitmapPage(insertVals, insertBitmaps).serialize(strTableName, strColName + page++);
//                index = border;
//            }
//        }
//        catch (Exception e)
//        {
//            e.printStackTrace();
//            throw new DBAppException("Error while making index");
//        }
//    }
//
//    public void insertIntoTable(String strTableName,
//                                Hashtable<String,Object> htblColNameValue) throws DBAppException
//    {
//        TableTuple pair = getColumns(strTableName);
//        Hashtable<String, String> columns = pair.columns;
//        String key = pair.key;
//        if (htblColNameValue.get(key) == null)
//            throw new DBAppException("The clustering key does not have a value");
//        htblColNameValue = validateColumns(columns, htblColNameValue);
//
//        Comparator<Hashtable<String, Object>> comparator = null;
//        try
//        {
//            Class<?> cls = Class.forName(columns.get(key));
//            comparator =
//                    (a, b) -> {
//                        try {
//                            return (Integer) cls.getMethod("compareTo", cls).invoke(a.get(key), b.get(key));
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        return 0;
//                    };
//        }
//        catch (Exception e)
//        {
//            throw new DBAppException("");
//        }
//
//        Page previous, current = null;
//        try
//        {
//            for (int i = 0; true; ++i)
//            {
//                previous = current;
//                File file = new File("data/" + strTableName + i + ".ser");
//
//                // reached last page and still hasn't reached insertion point
//                if (!file.exists())
//                {
//                    file.createNewFile();
//                    Page produced = new Page();
//                    produced.addFirst(htblColNameValue);
//                    produced.serialize(strTableName + i);
//                    break;
//                }
//
//                FileInputStream fileIn = new FileInputStream(file);
//                ObjectInputStream in = new ObjectInputStream(fileIn);
//                current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
//                in.close();
//                fileIn.close();
//                int index = Arrays.binarySearch(current.toArray(), htblColNameValue, comparator);
//                index = index < 0 ? - index - 1 : index;
//
//                if (index == current.size())
//                {
//                    //may be inserted in a coming page
//                    if (current.isFull())
//                        continue;
//
//                        //this is the last page and it's not full yet and insertion is in the end
//                    else {
//                        current.addLast(htblColNameValue);
//                        current.serialize(strTableName + i);
//                    }
//                }
//
//                //this is the page with insertion point but it's full and records need
//                //to be shifted through pages
//                else if (current.isFull())
//                {
//                    Hashtable<String, Object> propagate = current.removeLast(), temp;
//                    current.add(index, htblColNameValue);
//                    current.serialize(strTableName + i);
//                    int j;
//
//                    for (j = i + 1; (file = new File("data/" + strTableName + j + ".ser")).exists(); ++j)
//                    {
//                        fileIn = new FileInputStream(file);
//                        in = new ObjectInputStream(fileIn);
//                        current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
//                        in.close();
//                        fileIn.close();
//
//                        if (current.isFull())
//                        {
//                            temp = current.removeLast();
//                            current.addFirst(propagate);
//                            current.serialize(strTableName + j);
//                            propagate = temp;
//                        }
//                        else
//                        {
//                            current.addFirst(propagate);
//                            current.serialize(strTableName + j);
//                            propagate = null;
//                            break;
//                        }
//                    }
//
//                    //shifted all records but last one have no page to be put in
//                    if (propagate != null)
//                    {
//                        current = new Page();
//                        current.addFirst(propagate);
//                        current.serialize(strTableName + j);
//                    }
//                }
//
//                //this is last page and is not full yet and insertion is in the middle
//                else
//                {
//                    current.add(index, htblColNameValue);
//                    current.serialize(strTableName + i);
//                }
//                break;
//            }
//        }
//        catch (Exception e)
//        {
//            throw new DBAppException(e.getClass().getName() + ": " + e.getMessage());
//        }
//    }
//
//    public void updateTable(String strTableName, Object keyVal, Hashtable<String,Object> htblColNameValue ) throws DBAppException {
//        TableTuple pair = getColumns(strTableName);
//        Hashtable<String, String> columns = pair.columns;
//        String key = pair.key;
//        validateColumns(columns, htblColNameValue);
//        try { Class.forName(columns.get(key)).cast(keyVal);}
//        catch (ClassNotFoundException nf)
//        {
//            throw new DBAppException("The type of the key is not recognized");
//        }
//        catch (ClassCastException cc)
//        {
//            throw new DBAppException("The keyVal type is not compatible with the stored type of the key");
//        }
//
//        Comparator<Hashtable<String, Object>> comparator = null;
//        try
//        {
//            Class<?> cls = Class.forName(columns.get(key));
//            comparator =
//                    (a, b) -> {
//                        try {
//                            return (Integer) cls.getMethod("compareTo", cls).invoke(a.get(key), b.get(key));
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        return 0;
//                    };
//        }
//        catch (Exception e)
//        {
//            throw new DBAppException("");
//        }
//
//        Page current = null;
//        try {
//            for (int i = 0; true; ++i) {
//                File file = new File("data/" + strTableName + i + ".ser");
//                // reached last page and still hasn't reached insertion point
//                if (!file.exists()) {
//                    System.out.println("No record to update");
//                    return;
//                }
//                FileInputStream fileIn = new FileInputStream(file);
//                ObjectInputStream in = new ObjectInputStream(fileIn);
//                current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
//                in.close();
//                fileIn.close();
//                int index = Arrays.binarySearch(current.toArray(), htblColNameValue, comparator);
//                if(index>=0){
//                    Hashtable <String,Object> record = current.get(index);
//                    deleteFromTable(strTableName,record);
//                    for(String column:htblColNameValue.keySet()){
//                        record.put(column, htblColNameValue.get(column));
//                    }
//                    insertIntoTable(strTableName,record);
//                    System.out.println("Record updated successfully");
//                    return;
//                }else{
//                    if(index==-current.size()-1){
//                        continue;
//                    }else{
//                        System.out.println("error 404: record not found");
//                        return;
//                    }
//                }
//
//            }
//        } catch (Exception e) {
//            throw new DBAppException(e.getClass().getName() + ": " + e.getMessage());
//        }
//    }
//
//    public void deleteFromTable(String strTableName,
//                                Hashtable<String,Object> htblColNameValue) throws DBAppException
//    {
//        TableTuple pair = getColumns(strTableName);
//        Hashtable<String, String> columns = pair.columns;
//        String key = pair.key;
//        if (htblColNameValue.get(key) == null)
//            throw new DBAppException("The clustering key does not have a value");
//        htblColNameValue = validateColumns(columns, htblColNameValue);
//
//        Comparator<Hashtable<String, Object>> comparator = null;
//        try
//        {
//            Class<?> cls = Class.forName(columns.get(key));
//            comparator =
//                    (a, b) -> {
//                        try {
//                            return (Integer) cls.getMethod("compareTo", cls).invoke(a.get(key), b.get(key));
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        return 0;
//                    };
//        }
//        catch (Exception e)
//        {
//            throw new DBAppException("");
//        }
//
//        Page current = null;
//        try
//        {
//            for (int i = 0; true; ++i)
//            {
//                File file = new File("data/" + strTableName + i + ".ser");
//
//                if (!file.exists())
//                {
//                    break;
//                }
//
//                FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".ser");
//                ObjectInputStream in = new ObjectInputStream(fileIn);
//                current = new Page((Vector<Hashtable<String, Object>>) in.readObject());
//                in.close();
//                fileIn.close();
//                int index = Arrays.binarySearch(current.toArray(), htblColNameValue, comparator);
//
//                if (index == current.size())
//                    continue;
//
//                if (current.isEmpty())
////                    if (previous == null || previous.isFull())
//                {
//                    Page produced = new Page();
//                    produced.addFirst(htblColNameValue);
//
//                    int last;
//                    for (last = i+1; new File("data/" + strTableName + last + ".ser").exists(); ++last);
//
//                    while (last > i)
//                    {
//                        File old = new File("data/" + strTableName + (last-1) + ".ser");
//                        old.renameTo(new File("data/" + strTableName + last-- + ".ser"));
//                    }
//                    new File("data/" + strTableName + i + ".ser").createNewFile();
//                    produced.serialize(strTableName + i);
//                }
////                    else
////                    {
////                        previous.add(index, htblColNameValue);
////                        previous.serialize(strTableName + (i-1));
////                    }
//                else
//                {
//                    current.add(index, htblColNameValue);
//                    current.serialize(strTableName + i);
//                }
//                break;
//            }
//        }
//        catch (Exception e)
//        {
//            throw new DBAppException(e.getClass().getName() + ": " + e.getMessage());
//        }
//    }
//
//    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
//        TableTuple tuple = validateSQLQuery(arrSQLTerms);
//        Hashtable<String,Integer> iterate = new Hashtable<>();
//        ArrayList<StringBuilder>  bitmaps = new ArrayList<>();
//        for(int i=0;i<strarrOperators.length;i++){
//            if(tuple.IndexedColumns.contains(arrSQLTerms[i]._strTableName)){
//                switch(arrSQLTerms[i]._strOperator){
//                    case "==": bitmaps.add(getExact(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,true));break;
//                    case "!=": bitmaps.add(getExact(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,false));break;
//                    case ">": bitmaps.add(getRange(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,false,false));break;
//                    case ">=": bitmaps.add(getRange(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,false,true)); break;
//                    case "<": bitmaps.add(getRange(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,true,false)); break;
//                    case "<=":bitmaps.add(getRange(arrSQLTerms[i]._strTableName,arrSQLTerms[i]._strColumnName,arrSQLTerms[i]._objValue,true,true)); break;
//                }
//            }else{
//                iterate.add(arrSQLTerms[i]._strColumnName,i);
//                bitmaps.add(new StringBuilder());
//            }
//        }
//        String tableName = arrSQLTerms[0]._strTableName;
//        File file = null;
//        for(int i=0;(file=new File("data/" + tableName + i + ".ser")).exists();++i)
//        {
//            FileInputStream fileIn = new FileInputStream(file);
//            ObjectInputStream in = new ObjectInputStream(fileIn);
//            Page page = new Page((Vector<Hashtable<String, Object>>) in.readObject());
//            for(int j=0;j<=page.size();j++){
//                for(int k=0;k<=iterate.size();k++){
//                    if(page.get(j).){
//
//                    }
//                }
//            }
//            in.close();
//            fileIn.close();
//        }
//        ArrayList<String> bitwiseOperators = new ArrayList<>(strarrOperators.length);
//        for(String op : strarrOperators){
//            bitwiseOperators.add(op)
//        }
//
//        for(int i=0;i<bitwiseOperators.size();i++){
//            if(bitwiseOperators.get(i).equals("AND")){
//                bitmaps.set(i, BitmapPage.AND(bitmaps.get(i), bitmaps.get(i+1)));
//                bitmaps.remove(i+1);
//                bitwiseOperators.remove(i--);
//            }
//        }
//
//        for(int i=0;i<bitwiseOperators.size();i++){
//            if(bitwiseOperators.get(i).equals("OR")){
//                bitmaps.set(i, BitmapPage.OR(bitmaps.get(i), bitmaps.get(i+1)));
//                bitmaps.remove(i+1);
//                bitwiseOperators.remove(i--);
//            }
//        }
//
//        for(int i=0;i<bitwiseOperators.size();i++) {
//            if (bitwiseOperators.get(i).equals("XOR")) {
//                bitmaps.set(i, BitmapPage.XOR(bitmaps.get(i), bitmaps.get(i + 1)));
//                bitmaps.remove(i + 1);
//                bitwiseOperators.remove(i--);
//            }
//        }
//        for(int i=0;(file=new File("data/" + tableName + i + ".ser")).exists();++i)
//        {
//            FileInputStream fileIn = new FileInputStream(file);
//            ObjectInputStream in = new ObjectInputStream(fileIn);
//            Page page = new Page((Vector<Hashtable<String, Object>>) in.readObject());
//            for(int j=0;j<page.size();j++){
//                if(bitmaps.get(0).charAt(i)==1){
//
//                }
//            }
//            in.close();
//            fileIn.close();
//        }
//
//        return null;
//    }
//
//    public TableTuple validateSQLQuery(SQLTerm[] arrSQLTerms,
//                                       String[] strarrOperators) throws DBAppException
//    {
//        if (arrSQLTerms.length != strarrOperators.length + 1)
//            throw new DBAppException("The number of SQL terms and operators are not compatible");
//
//        for (SQLTerm term : arrSQLTerms)
//            if (!availableOpInside.contains(term._strOperator))
//                throw new DBAppException("");
//
//        for (String operator : strarrOperators)
//            if (!availableOpOutside.contains(operator))
//                throw new DBAppException("The " + operator + " operator is not available");
//    }
//
//    public TableTuple getColumns(String tableName) throws DBAppException
//    {
//        Hashtable<String, String> columns = new Hashtable<>();
//        String key = "";
//
//        BufferedReader fileReader = null;
//
//        try
//        {
//            String line;
//            boolean keySet = false;
//            fileReader = new BufferedReader(new FileReader("data/metadata.csv"));
//
//            fileReader.readLine();
//
//            while ((line = fileReader.readLine()) != null)
//            {
//                String[] tokens = line.split(",");
//
//                if (tokens.length == 5 )
//                    if (tokens[0].equals(tableName))
//                    {
//                        columns.put(tokens[1], tokens[2]);
//                        if (tokens[3].equals("True"))
//                            if (keySet)
//                                throw new DBAppException("No multi keys are allowed");
//                            else
//                            {
//                                key = tokens[1];
//                                keySet = true;
//                            }
//                    }
//                    else
//                        break;
//            }
//
//            if (columns.isEmpty())
//                throw new DBAppException("There's no table with that name");
//
//            if (!keySet)
//                throw new DBAppException("There's no key column");
//        }
//        catch (DBAppException dbe) { throw dbe; }
//        catch (Exception e) { e.printStackTrace(); }
//        finally
//        {
//            try { fileReader.close(); }
//            catch (IOException e) { e.printStackTrace(); }
//        }
//
//        return new TableTuple(columns, key);
//    }
//
//    public Hashtable<String, Object> validateColumns(Hashtable<String, String> tamplet,
//                                                     Hashtable<String, Object> givin)
//            throws DBAppException
//    {
//        if (tamplet.size() < givin.size())
//            throw new DBAppException("The cardinality of the input does not mach the table's");
//
//        for (String column : givin.keySet())
//        {
//            if (!tamplet.containsKey(column))
//                throw new DBAppException("The input row contains invalid column " + column);
//
//            Object value = givin.get(column);
//            if (value != null)
//            {
//
//                String tampletClass = tamplet.get(column),
//                        givinClass = value.getClass().getCanonicalName();
//                if (!tampletClass.equals(givinClass))
//                    throw new DBAppException("The column " + column + " should have been " + tampletClass + ", but it was " + givinClass);
//            }
//        }
//
//        Hashtable<String, Object> result = (Hashtable<String, Object>) givin.clone();
//
//        for (String column : tamplet.keySet())
//            if (!result.containsKey(column))
//                result.put(column, null);
//
//        return result;
//    }
//
//    public static class TableTuple
//    {
//        public Hashtable<String, String> columns;
//        public String key;
//        public Hashtable<String>IndexedColumns
//
//        public TablePair(Hashtable<String, String> columns,
//                         String key,Hashtable<String> IndexedColumns)
//        {
//            this.columns = columns;
//            this.key = key;
//            this.IndexedColumns =IndexedColumns;
//        }
//    }
//}
