import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Tag;
import org.jsoup.select.Elements;


/**
 * Created by Saravana on 5/25/2016.
 */
public class SongsDownload
{
//	final static String HOME = "http://178.33.231.157";
	final static String HOME = "http://db.oruwebsite.com";
	//	static String folder = "/Tamil?dir=46 - Vijay All Film Songs Download";
	static String folder = "/Tamil?dir=";
	static String targetFolder = "D:/songs/";
	static DownloadDetails downloadDetails;
	static ConcurrentNavigableMap<String,Element> pendingWork = new ConcurrentSkipListMap<>();
	static ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue(20000);
	private static ExecutorService executorService = new ThreadPoolExecutor(32, 32,
		120, TimeUnit.SECONDS,
		queue);
	static Set<String> completedDir = new HashSet<>();

	private static CacheService cacheService = new MongoCacheProvider();
	//garbage extension in server
	Set<String> garbageExtensions = new HashSet<>();

	{
		garbageExtensions.add("ini");
		garbageExtensions.add("html");
		garbageExtensions.add("bc!");
		garbageExtensions.add("m3u");
		garbageExtensions.add("php");
		garbageExtensions.add("X");
		garbageExtensions.add("listing");
	}

	public static void main(String[] args) throws Throwable
	{
		SongsDownload songsDownload = new SongsDownload();
		downloadDetails = new DownloadDetails();
		Attributes attributes = new Attributes();
		attributes.put(new Attribute("href", "http://db.oruwebsite.com/Tamil?dir="));
//		pendingWork.add(new Element(Tag.valueOf("a"), "http://db.oruwebsite.com/Tamil?dir=", attributes));
		pendingWork.put("http://db.oruwebsite.com/Tamil?dir=", new Element(Tag.valueOf("a"), "http://db.oruwebsite.com/Tamil?dir=", attributes));
		while (pendingWork.size() > 0)
		{
			Element value = null;
			try
			{

//			new SongsDownload().listFiles(null)
				value = pendingWork.pollFirstEntry().getValue();
				songsDownload.listFiles(value)
				.map(e -> e.attr("href"))
				.map(s -> HOME + "/Tamil" + s.substring(1))
				.filter( u -> !downloadDetails.isExist(u.replaceAll(" ", "%20")))
				.peek(u -> downloadDetails.put(u.replaceAll(" ", "%20")))
				.map(u -> (Runnable) () -> downloadSong(u))
				.forEach(executorService::submit);
			}
			catch (RejectedExecutionException e)
			{
				pendingWork.put(value.attr("href"),value);
				TimeUnit.MINUTES.sleep(1);
			}
			//.peek(System.out::println)

//		String folder = "/Tamil?dir=";
//		new SongsDownload().listFiles(HOME, folder)//.peek(System.out::println)
//			.map(e -> e.attr("href"))
//			.map(s -> HOME + "/Tamil" + s.substring(1))
//			.map(u -> (Runnable) () -> downloadSong(u))
//			.forEach(executorService::submit);
//			.forEach(SongsDownload::downloadSong);
//			.forEach(System.out::println);
//			.map( s ->  s.substring(s.lastIndexOf('.')))
//			.collect(Collectors.groupingBy(Function.identity(),Collectors.counting()))
//			.forEach( (k,v) -> System.out.println(k+" "+v) );
		}
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Shuting down~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		executorService.awaitTermination(30,TimeUnit.MINUTES);
	}

	private static void downloadSong(String url)
	{
		String songURL = url.replaceAll(" ", "%20");
		File localFileName = new File(targetFolder, url.substring(HOME.length() + 1));
		localFileName.mkdirs();
//		if (false == downloadDetails.isExist(songURL))
		{
			int size = Integer.parseInt(cacheService.getVo(songURL, u -> getSize(u)+""));
			downloadFile(songURL, size, localFileName);
		}
	}

	private static int getSize(String url)
	{
		try
		{
			HttpURLConnection urlConnection = null;
			InputStream inputStream = null;
			try
			{
				urlConnection = (HttpURLConnection) new URL(url).openConnection();
				urlConnection.setRequestMethod("HEAD");
				inputStream = urlConnection.getInputStream();
				return urlConnection.getContentLength();
			}
			finally
			{
				inputStream.close();
				urlConnection.disconnect();
			}
		}
		catch (Throwable e)
		{
			System.err.println("Failed to get url "+url);
			return -1;
		}
	}

	private static void downloadFile(String songUrl, long fileSize, File localFileName)
	{
		if (localFileName.exists())
		{
			if (localFileName.length() == fileSize)
			{
				downloadDetails.put(songUrl);
				return;
			}
			System.err.println("Size mismatch "+songUrl);
		}
		localFileName.delete();
		long startTime = System.currentTimeMillis();
//		System.out.println("Downloading " + songUrl);
		int retry = 0;
		while (!copyURLToFile(songUrl, fileSize, localFileName) && retry++ < 3)
		{
			;
		}
		if(retry>2)
		{
			downloadDetails.remove(songUrl);
		}
		else
		{
			long endTime = System.currentTimeMillis();
			System.out.println(
				"Downloaded " + songUrl.replaceAll("%20", " ") + " in " + ((endTime - startTime) / 60000) + ":" +
					((endTime - startTime) / 1000) % 60 + "s    ");// + "To : " + localFileName);
			downloadDetails.put(songUrl);
		}
	}

	public static boolean copyURLToFile(String songUrl, long fileSize, File localFileName)
	{
		BufferedOutputStream output = null;
		InputStream input = null;
		try
		{
			byte[] buffer = new byte[2 * 1024 * 1024];
			input = new URL(songUrl).openStream();
			output = new BufferedOutputStream(new FileOutputStream(localFileName), 2 * 1024 * 1024);
			int n;
			while (-1 != (n = input.read(buffer)))
			{
				output.write(buffer, 0, n);
//				System.out.print("\rProgress -- " + (localFileName.length() * 100 / fileSize) + "%");
			}
//			System.out.println("\rProgress -- 100%");
		}
		catch (Throwable throwable)
		{
			System.err.println(Thread.currentThread().getId() + " Download failed for : " + songUrl);
			return false;
		}
		finally
		{
			closeStream(output);
			closeStream(input);
		}
		return localFileName.length() == fileSize;
	}

	private static void closeStream(Closeable closeable)
	{
		if (null != closeable)
		{
			try
			{
				closeable.close();
			}
			catch (IOException e)
			{
			}
		}
	}

	private Stream<Element> listFiles(Element element)
	{
		Document doc = null;
		String url;
		String relativePath = element.attr("href");
		if (relativePath.startsWith(HOME))
		{
			url = relativePath.trim().replaceAll(" ", "%20");
		}
		else
		{
			url = HOME + relativePath.trim().replaceAll(" ", "%20");
		}
		if(completedDir.contains(url))
			return Stream.empty();
		completedDir.add(url);
		try
		{
//			Future<Document> submit = executorService.submit();
			doc = new RetreiveDocument(url).call();
		}
		catch (Exception e)
		{
			System.out.println("Failed to load URL : " + url);
			return Stream.empty();
		}
		Elements select = doc.select("td > a"); //get td with immediate "a" tag.
		Predicate<Element> garbageEntries = e -> e.ownText().contains("Parent Directory");
		Predicate<Element> existExtn = e -> e.ownText().lastIndexOf('.') != -1;
		Predicate<Element> garbagePredicate = garbageEntries.or(existExtn
			.and(e -> garbageExtensions.contains(e.ownText().substring(e.ownText().lastIndexOf('.') + 1))));
		garbagePredicate = garbagePredicate.or(e -> e.ownText().contains("#"));

		List<Element> list = select.stream().filter(garbagePredicate.negate())
			.peek(e ->
			{
				if (e.attr("href").endsWith("/"))
				{
					pendingWork.put(e.attr("href"),e);
				}
			})
			.filter(e -> !e.attr("href").endsWith("/"))
			.collect(Collectors.toList());
//			.flatMap(e -> e.attr("href").endsWith("/") ? listFiles(home, e.attr("href")) : Stream.of(e));
		return list.stream();
	}
//
//	private Stream<Element> listFiles(String home, String relativePath)
//	{
//		Document doc = null;
//		String url;
//		if(relativePath.startsWith(home))
//		{
//			 url = relativePath.trim().replaceAll(" ", "%20");
//		}
//		else
//		{
//			url = home + relativePath.trim().replaceAll(" ", "%20");
//		}
//		try
//		{
//			Future<Document> submit = executorService.submit(new RetreiveDocument(url));
//			doc = submit.get(30, TimeUnit.SECONDS);
//		}
//		catch (Exception e)
//		{
//			System.out.println("Failed to load URL : " + url);
//			return Stream.empty();
//		}
//		Elements select = doc.select("td > a"); //get td with immediate "a" tag.
//		Predicate<Element> garbageEntries = e -> e.ownText().contains("Parent Directory");
//		Predicate<Element> existExtn = e -> e.ownText().lastIndexOf('.') != -1;
//		Predicate<Element> garbagePredicate = garbageEntries.or(existExtn
//			.and(e -> garbageExtensions.contains(e.ownText().substring(e.ownText().lastIndexOf('.') + 1))));
//		garbagePredicate=garbagePredicate.and( e -> e.ownText().contains("#"));
//
//		Stream<Element> stream = select.stream().filter(garbagePredicate.negate())
//			.peek( e -> {
//				if(false == e.attr("href").endsWith("/"))
//				{
//					pendingWork.add(Stream.of(e));
//				}
//			} )
//			.filter(e -> e.attr("href").endsWith("/"))
//			.flatMap(Stream::of);
////			.flatMap(e -> e.attr("href").endsWith("/") ? listFiles(home, e.attr("href")) : Stream.of(e));
//		return stream;
//	}

	private static class DownloadDetails
	{
		AtomicInteger existingCount;
		File file = new File("DownloadInfo.txt");
		Set<String> songlist = new ConcurrentSkipListSet<>();

		DownloadDetails() throws Exception
		{
			if (file.exists() == false)
			{
				file.createNewFile();
			}
			((List<String>) FileUtils.readLines(file)).stream().forEach(songlist::add);
			existingCount = new AtomicInteger(songlist.size());
			new Thread(this::persist).start();
		}

		public boolean isExist(String url)
		{
			return songlist.contains(url);
		}

		private void persist()
		{
			while (true)
			{
				try
				{
					TimeUnit.SECONDS.sleep(15);
				}
				catch (Exception e)
				{
				}
				int size = songlist.size();
				if (size > existingCount.intValue())
				{
					try
					{
						FileUtils.writeLines(file, songlist);
					}
					catch (Exception exce)
					{
						exce.printStackTrace();
					}
					existingCount.set(size);
				}
			}
		}

		public void put(String songUrl)
		{
			songlist.add(songUrl);
		}

		public void remove(String songUrl)
		{
			songlist.remove(songUrl);
		}
	}

	private class RetreiveDocument implements Callable<Document>
	{
		private String url;

		public RetreiveDocument(String url)
		{
			this.url = url;
		}

		@Override public Document call() throws Exception
		{
			String vo = cacheService.getVo(url, u -> Jsoup.connect(u).get().toString());
			return Jsoup.parse(vo);
		}
	}

	interface CachePopulator
	{
		String buildVO(String url) throws Exception;
	}

	static interface CacheService
	{
		String getVo(String url,CachePopulator cachePopulator);
	}

	static class MongoCacheProvider implements  CacheService
	{
		MongoCollection<org.bson.Document> cache = new MongoClient().getDatabase("songs").getCollection("cache");
		public String getVo(String url, CachePopulator cachePopulator)
		{
			String vo = null;
			try
			{
				FindIterable<org.bson.Document> vos = cache.find(Filters.eq("url", url));
				MongoCursor<org.bson.Document> iterator = vos.iterator();
				if (iterator.hasNext())
				{
					org.bson.Document cachedObject = iterator.next();
					vo = cachedObject.get("vo").toString();
				}
				else
				{
					vo = cachePopulator.buildVO(url);
					org.bson.Document cacheObject = new org.bson.Document();
					cacheObject.put("url", url);
					cacheObject.put("vo", vo);
					cache.insertOne(cacheObject);
				}
			}
			catch (Throwable e)
			{
				e.printStackTrace();
			}
			return vo;
		}
	}

}
