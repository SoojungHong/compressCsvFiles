import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream, ZipEntry, ZipOutputStream}
import java.io.{BufferedReader, File, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions.input_file_name
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


object ArchiveCsv {

  def main(args: Array[String]): Unit = {
    val isRunLocal = false
    if(isRunLocal) {
      runLocal()
    } else {
      val conf = new SparkConf().setAppName("Archive Application")
      val sc = new SparkContext(conf)

      //---------------------------------------
      // read .csv files in HDFS in given path
      //---------------------------------------
      //val path = "/tmp/R/ec_dev/J_CORE_SEQUENCE_ROOT_CAUSE/"
      val path = "/user/hive/warehouse/post39"
      //val out = "hdfs:///mnsent.rubcom.ch:8020/user/hive/warehouse/post39/out.gz"
      val out = "/user/hive/warehouse/post39/myout.gz"

      readFilesInPath(path, out, sc)
    }
  }


  //--------------------------------
  // file traverse in HDFS Hadoop
  //--------------------------------
  def readFilesInPath(path:String, out:String, sc:SparkContext): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val p = new Path(path)
    val ls = fs.listStatus(p)
    val files : Iterable[String] = ls.map(_.getPath.toString)

    //ls.foreach( x => println(x.getPath) ) //traverse each files in given path
    //ls.foreach( filename => files.toBuffer += filename.getPath)
    println("======================= DEBUG : files:Iterable[String] ===================")
    files.foreach(file => println(file))
    println("==========================================================================")
    val isMerge = false
    if(isMerge != true) {
      createFile(out, files) //: FSDataOutputStream = {
      val filename = "/user/hive/warehouse/post39/peas_small.csv"
      readFilesAndMerge(createFile(out, files), filename)

      }
    }

  //-------------------------------------
  // Get list of files under given path
  //-------------------------------------
  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  def getListOfFilesOnLocal(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }


  //-----------------------------
  // local csv file compression
  //-----------------------------
  def runLocal(): Unit = {

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val p = new Path("/test")
    val ls = fs.listStatus(p)

    val okFileExtensions = List("csv") //List("txt", "csv") //at the moment, we compress csv files only
    val files = getListOfFilesOnLocal(new File("/test"), okFileExtensions)
    val nfiles : Iterable[String] = files.map(_.getPath)

    nfiles.foreach(x => println(x))

    val out = "/test/zip/test.zip"
    //zip(out, nfiles)

  }

  //---------------------------------------------------
  // compress byte output stream (bos) to output byte
  //---------------------------------------------------
  def compress(input : Array[Byte]):Array[Byte] = {
    val bos = new ByteArrayOutputStream(input.length)
    val gzip = new GZIPOutputStream(bos)
    gzip.write(input)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  //------------------------
  // create file in HDFS
  //------------------------
  def createFile(out: String, files: Iterable[String]): FSDataOutputStream = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://mnsent.rubcom.ch:8020")
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path(out)) //"/user/hive/warehouse/post39/test.gz"))

    output
  }

  def readFilesAndMerge(outputDS : FSDataOutputStream, filename : String): Unit = {
    //in HDFS
    val hdfs = FileSystem.get(new java.net.URI("hdfs://mnsent.rubcom.ch:8020/"), new Configuration)
    val path = new Path("/user/hive/warehouse/post39/peas_small.csv")
    val stream = hdfs.open(path)
    def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))
    import scala.compat.Platform.EOL
    readLines.takeWhile(_ != null).foreach(line => {
      outputDS.write(line.getBytes)
      outputDS.write(EOL.getBytes)})

    /* //read other file and add
    val hdfs1 = FileSystem.get(new java.net.URI("hdfs://mnsent.rubcom.ch:8020/"), new Configuration)
    val path1 = new Path("/user/hive/warehouse/post39/peas.csv")
    val stream1 = hdfs1.open(path1)
    def readLines1 = Stream.cons(stream1.readLine, Stream.continually( stream1.readLine))
    //readLines.takeWhile(_ != null).foreach(line => println(line))
    //readLines.takeWhile(_ != null).foreach(line => outputDS.write(line.getBytes))
    import scala.compat.Platform.EOL
    readLines1.takeWhile(_ != null).foreach(line => {
      outputDS.write(line.getBytes)
      outputDS.write(EOL.getBytes)})
    */
  }

  def __zip(out: String, files: Iterable[String]) = {
    import java.io.{ BufferedInputStream, FileInputStream, FileOutputStream }
    import java.util.zip.{ ZipEntry, ZipOutputStream }

    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://mnsent.rubcom.ch:8020")
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path("/user/hive/warehouse/post39/output.gz"))

    val zip = new ZipOutputStream(new FileOutputStream(output.toString))

    files.foreach { name =>
      zip.putNextEntry(new ZipEntry(name))
      val in = new BufferedInputStream(new FileInputStream(name))
      var b = in.read()
      while (b > -1) {
        zip.write(b)
        b = in.read()
      }
      in.close()
      zip.closeEntry()
    }
    zip.close()
  }

  def _zip(out: String, files: Iterable[String]) = {
    import java.io.{ BufferedInputStream, FileInputStream, FileOutputStream }
    import java.util.zip.{ ZipEntry, ZipOutputStream }

    val zip = new ZipOutputStream(new FileOutputStream(out))

    files.foreach { name =>
      zip.putNextEntry(new ZipEntry(name))
      val in = new BufferedInputStream(new FileInputStream(name))
      var b = in.read()
      while (b > -1) {
        zip.write(b)
        b = in.read()
      }
      in.close()
      zip.closeEntry()
    }
    zip.close()
  }

}
