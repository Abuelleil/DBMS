import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class WriteProperties
{
    public static void main(String[] args) {
        Properties prop = new Properties();
        OutputStream output = null;

        try
        {
            output = new FileOutputStream("config/DBApp.config");

            prop.setProperty("MaximumRowsCountinPage", "200");
            prop.setProperty("BitmapSize", "15");

            prop.store(output, null);
        }
        catch (IOException io)
        {
            io.printStackTrace();
        }
        finally
        {
            if (output != null)
            {
                try
                {
                    output.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }

        }
    }
}
