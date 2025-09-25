import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HDFSJavaAPIWriteDemo {
	
	public static void createDirectory() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        String directoryName = "BigData/HDFSExample";
        Path path = new Path(directoryName);
        fileSystem.mkdirs(path);
    }
	
	public static void writeFileToHDFS() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
       
        String fileName = "read_write_hdfs_example.txt";
        Path hdfsWritePath = new Path("/user/BigData/HDFSExample/" + fileName);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
        bufferedWriter.write("Java API to write data in HDFS");
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    }
	
	public static void appendToHDFSFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        configuration.setBoolean("dfs.support.append", true);
        FileSystem fileSystem = FileSystem.get(configuration);
       
        
        String fileName = "read_write_hdfs_example.txt";
        Path hdfsWritePath = new Path("/user/BigData/HDFSExample/" + fileName);
        Boolean isAppendable = Boolean.valueOf(fileSystem.getConf().get("dfs.support.append"));
      
        FSDataOutputStream fsDataOutputStream = fileSystem.append(hdfsWritePath);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
        if(isAppendable) {
        	bufferedWriter.write("Java API to append data in HDFS file");
        }
        else {
            System.out.println("Please set the dfs.support.append property to true");
        }
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    }
	
	public static void readFileFromHDFS() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        //Create a path
        String fileName = "read_write_hdfs_example.txt";
        Path hdfsReadPath = new Path("/user/BigData/HDFSExample/" + fileName);
        //Init input stream
        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
        //Classical input stream usage
        String out= IOUtils.toString(inputStream, "UTF-8");
        System.out.println(out);
        /*BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        String line = null;
        while ((line=bufferedReader.readLine())!=null){
            System.out.println(line);
        }*/
        inputStream.close();
        fileSystem.close();
    }
	
	
	
	public static void main(String[] args) throws Exception{
		//createDirectory();
		//writeFileToHDFS();
		//readFileFromHDFS();
		
	}
}
