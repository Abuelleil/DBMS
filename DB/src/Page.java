import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable
{
    private Vector<Hashtable<String, Object>> records;
    private static int maxCapacity;
    private static boolean maxCapacitySet = false;

    public Page(Vector<Hashtable<String, Object>> records)
    {
        this.records = records;
    }

    public Page()
    {
        this(new Vector<>());
    }

    public static void setMaxCapacity(int maxCapacity)
    {
        if (maxCapacitySet)
            System.out.println("The max capacity is already set");

        else
        {
            Page.maxCapacity = maxCapacity;
            maxCapacitySet = true;
        }
    }

    public boolean isFull()
    {
        return records.size() >= maxCapacity;
    }

    public boolean isEmpty()
    {
        return records.isEmpty();
    }

    public void add(int index, Hashtable<String, Object> row)
    {
        records.add(index, row);
    }

    public void addFirst(Hashtable<String, Object> row)
    {
        records.add(0, row);
    }

    public void addLast(Hashtable<String, Object> row) { records.addElement(row); }

    public Hashtable<String, Object> get(int index)
    {
        return records.get(index);
    }

    public Hashtable<String, Object> getFirst()
    {
        return records.firstElement();
    }

    public Hashtable<String, Object> getLast()
    {
        return records.lastElement();
    }

    public Hashtable<String, Object> remove(int index) { return records.remove(index); };

    public Hashtable<String, Object> removeFirst() { return records.remove(0); };

    public Hashtable<String, Object> removeLast() { return records.remove(records.size() - 1); };

    public Hashtable<String, Object>[] toArray()
    {
        return records.toArray(new Hashtable[0]);
    }

    public int size()
    {
        return records.size();
    }

    public static int getMaxCapacity() { return maxCapacity; }

    public void serialize(String name) throws DBAppException
    {
        try
        {
            File file = new File("data/" + name + ".ser");
            if (file.exists())
            {
                file.delete();
            }

            file.createNewFile();
            FileOutputStream fileOut = new FileOutputStream(file);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(records);
            out.close();
            fileOut.close();
        }
        catch (IOException e)
        {
            throw new DBAppException("An error occured while serializing page " + name);
        }
    }
}
