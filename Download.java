import java.io.*;
import java.net.*;

/**
 * Created with IntelliJ IDEA.
 * User: vasanth
 * Date: 10/3/13
 * Time: 9:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class Download {

//        public static final String PATH = "http://db.oruwebsite.com/Tamil/Songs/37 - Gaana Songs";
//    public static final String PATH = "http://db.oruwebsite.com/Tamil/Songs/40 - Crazy Mohan Dramas";
    public static final String PATH = "http://db.oruwebsite.com/Tamil/Songs/01 - New Songs/Year (2015)";
    private static final String BASEFolder = "d:/gaana";
    private static boolean isFailed;

    public static void main(String[] args) throws Exception {
        do {
            isFailed = false;
            downloadURL(PATH);
        } while (isFailed);
    }

    private static void downloadURL(String baseUrl) throws IOException {
        String[] fileDetails = listFileDetails(baseUrl);
        for (String fileDetail : fileDetails) {
            //split the directory listing
            String[] fileAttributes = fileDetail.trim().split(" +", 9);
            String fileName = fileAttributes[fileAttributes.length - 1];
            long fileSize = new Long(fileAttributes[4].trim());
            //download the directory
            if (false == fileName.endsWith(".listing") && false == fileDetail.startsWith("d")) {
                downloadFile(baseUrl, fileName, fileSize);
                continue;
            }
            //if not directory download it
            if (fileDetail.startsWith("d") && !fileName.endsWith(".")) {
                downloadURL(baseUrl + "/" + fileName);
            }
        }
    }

    private static void downloadFile(String baseUrl, String fileName, long fileSize) throws MalformedURLException {
        String dirName = baseUrl.substring(PATH.length());
        File folderName = new File(BASEFolder + "/" + dirName);
        if (false == folderName.exists()) {
            folderName.mkdirs();
        }
        URL songUrl = new URL((baseUrl + "/" + fileName).replaceAll(" ", "%20"));
        File localFileName = new File(folderName, fileName);
        try {
            cleanLocalFile(localFileName, fileSize);
            downloadFile(fileName, songUrl, fileSize, localFileName);
        } catch (Throwable throwable) {
            isFailed = true;
            System.out.println("Download Failed : " + songUrl + "   " + localFileName.length());
            if (localFileName.exists()) {
                localFileName.delete();
            }
        }
    }

    private static String[] listFileDetails(String baseUrl) throws IOException {
        StringBuilder builder = new StringBuilder();
        URL url = new URL(baseUrl.trim().replaceAll(" ", "%20") + "/.listing");
        URLConnection connection = url.openConnection();
        DataInputStream inputStream = new DataInputStream(connection.getInputStream());
        byte[] buffer = new byte[1024 * 10];
        int size;
        while (-1 != (size = inputStream.read(buffer))) {
            builder.append(new String(buffer, 0, size));
        }
        inputStream.close();
        if (connection instanceof HttpURLConnection) {
            ((HttpURLConnection) connection).disconnect();
        }
        System.out.println(builder.toString());
        return builder.toString().split("\n");
    }

    private static void cleanLocalFile(File localFileName, long fileSize) throws IOException {
        if (localFileName.exists() && fileSize != localFileName.length()) {
            System.out.println("File Size mismatch : " + localFileName.getCanonicalPath());
            System.out.println(fileSize + "   " + localFileName.length());
            localFileName.delete();
        }
    }

    private static void downloadFile(String file, URL songUrl, long fileSize, File localFileName) throws IOException {
        if (false == localFileName.exists()) {
            long startTime = System.currentTimeMillis();
            System.out.println("\nDownloading " + file + "  " + fileSize + "  to    " + localFileName + "   ");
            copyURLToFile(songUrl, fileSize, localFileName);
            long endTime = System.currentTimeMillis();
            System.out.println("Download completed in " + ((endTime - startTime) / 60000) + ":" + ((endTime - startTime) / 1000) % 60 + "    ");
        }
    }

    public static void copyURLToFile(URL songUrl, long fileSize, File localFileName) throws IOException {
        BufferedOutputStream output = null;
        InputStream input = null;
        try {
            byte[] buffer = new byte[1024 * 1024];
            input = songUrl.openStream();
            output = new BufferedOutputStream(new FileOutputStream(localFileName), 2 * 1024 * 1024);
            int n;
            while (-1 != (n = input.read(buffer))) {
                output.write(buffer, 0, n);
                System.out.print("\rProgress -- " + (localFileName.length() * 100 / fileSize) + "%");
            }
            System.out.println("\rProgress -- 100%");
        } finally {
            closeStream(output);
            closeStream(input);
        }
    }

    private static void closeStream(Closeable closeable) {
        if (null != closeable) {
            try {
                closeable.close();
            } catch (IOException e) {
            }
        }
    }
//
//    private void testException(String param, String[] paramArray)
//    {
//        try
//        {
//            doOperation(param);
//        }catch (Exception exception)
//        {
//            log.error("operation failed for param:"+param,exception);
//        }
//        try
//        {
//            doOperationOnArray(paramArray);
//        }catch (Exception exception)
//        {
//            //concatenation array object over string will add the hashcode of the array.
//            //(we need the array contents for analyzing)
//            log.error("operation failed for param:"+ Arrays.asList(paramArray),exception);
//        }
//    }
//    private static long getMillis(String sValue)
//    {
//        if (sValue.endsWith("S")) {
//            return new ExtractSecond(sValue).invoke();
//        } else if (sValue.endsWith("ms")) {
//            return new ExtractMillisecond(sValue).invoke();
//        } else if (sValue.endsWith("s")) {
//            return new ExtractInSecond(sValue).invoke();
//        } else if (sValue.endsWith("m")) {
//            return new ExtractInMinute(sValue).invoke();
//        } else if (sValue.endsWith("H") || sValue.endsWith("h")) {
//            return new ExtractHour(sValue).invoke();
//        } else if (sValue.endsWith("d")) {
//            return new ExtractDay(sValue).invoke();
//        } else if (sValue.endsWith("w")) {
//            return new ExtractWeek(sValue).invoke();
//        } else {
//            return Long.parseLong(sValue);
//        }
//    }
//
//    static Map<String, Double> multipliers = new HashMap<>();
//    {
//        multipliers.put("S", 60.0 * 60);
//        multipliers.put("ms", 60.0 * 60 * 1000);
//        multipliers.put("m", 60.0);
//        multipliers.put("H", 1.0);
//        multipliers.put("d", 1.0 / 24);
//        multipliers.put("w", 1.0 / (24 * 7));
//    }
//    private static long getMillisModified(String sValue)
//    {
//        //divide the sValue into digit and word.
//        Pattern foo = Pattern.compile(".*(\\d+)\\s*(\\w+)");
//        Matcher bar = foo.matcher(sValue);
//        if(bar.matches())
//        {
//            return (long) (Double.parseDouble(bar.group(1)) * multipliers.get(bar.group(2)));
//        }
//        return Long.parseLong(sValue);
//    }
//
//
//    private void executeService(String lastName)
//    {
//        List<Accounts> accounts = getAccountsWithLastName(lastName);
//        if(null != accounts)
//        {
//            accounts.forEach(System.out::println);
//        }
//    }
//
//    List <Accounts> getAccountsWithLastName(String lastName)
//    {
//        List<Accounts> accounts = null;
//        try
//        {
//            accounts = new ArrayList<>();
//            //popuulate accounts
//            //...
//            //...
//            if(accounts.size()>0)
//                return accounts;
//        }
//        finally
//        {
//        }
//        //no accountr details retrn null
//        return null;
//    }
//
//
//    private void executeService(String lastName)
//    {
//        List<Accounts> accounts = getAccountsWithLastName(lastName);
//        accounts.forEach(System.out::println);
//    }
//
//    List <Accounts> getAccountsWithLastName(String lastName)
//    {
//        List<Accounts> accounts = new ArrayList<>();
//        try
//        {
//            //popuulate accounts
//            //...
//            //...
//        }
//        finally
//        {
//        }
//        return accounts;
//    }



}