import java.io.File
import java.nio.ByteBuffer

import FsClientTest.client
import org.apache.commons.io.IOUtils
import org.grapheco.regionfs.FileId
import org.grapheco.regionfs.client.FsClient
import org.grapheco.regionfs.util.ByteBufferConversions._
import org.junit.{BeforeClass, Test}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object FsClientTest {
  @BeforeClass
  val client = new FsClient("10.0.85.107:2181")
}

class FsClientTest {



  @Test
  def testWriteFile(): Unit = {
    /**
      * //写入字符串
      * val srcString: String = "it is a new new test string"
      * writeFile(srcString.getBytes)
      **/

/**
      //写入文件
      val srcFile: File = new File("D:/test/10KB.txt")
      Await.result(client.writeFile(srcFile), Duration("4s"))

**/

    /**
      * //读文件
      * val srcString: String = "i want to print hahaha"
      * //writeFile(srcString.getBytes)
      * val fileId = writeFile(srcString.getBytes)
      * println(fileId)
      * //Await.result(client.readFile(fileId,FileInputStream => System.out), Duration("4s"))
      * val futureStr: Future[String] = client.readFile(fileId,(is) => IOUtils.toString(is).asInstanceOf[String])
      * val str =Await.result(futureStr,Duration("10s"))
      * println(str)
      *client.close()
      * }
      **/

    /**
    val srcFile: File = new File("D:/regionfs-test/f1.txt")
    val array= ArrayBuffer[Int]();
    for (i <-1 to 10){
      val startTime =System.nanoTime()/1000
      client.writeFile(srcFile)
      val id =client.writeFile(srcFile)
      println(id)
      val endTime=System.nanoTime()/1000
      println("1个文件第"+i+"次写入时间： "+(endTime-startTime)+"μs");
      array+=(endTime-startTime).toInt

    }
    System.out.println("最短时间：" + array.min +"μs");
    System.out.println("最长时间：" +array.max +"μs");
    System.out.println("平均时间：" +array.sum/array.size +"μs");
      **/

    /**
    val srcFile: File = new File("D:/regionfs-test/f1.txt")
    val arraywrite= ArrayBuffer[Int]();
    val arrayread = ArrayBuffer[Int]();

    val startTime =System.currentTimeMillis()
    client.writeFile(srcFile)
    val endTime=System.currentTimeMillis()
    println("文件写入时间： "+(endTime-startTime)+"ms");

    val id =Await.result(client.writeFile(srcFile), Duration("4s"))
    println(id)

    arraywrite+=(endTime-startTime).toInt
    println("写入文件"+arraywrite)
    System.out.println("写入最短时间：" + arraywrite.min +"ms");
    System.out.println("写入最长时间：" +arraywrite.max +"ms");
    System.out.println("写入平均时间：" +arraywrite.sum/arraywrite.size +"ms");

    println("读取文件/-------------------------------------------------------------------------------------------------/")
    val t1=System.currentTimeMillis()
    readFile(id)
    val t2=System.currentTimeMillis()
    println("文件读取时间： "+(t2-t1)+"ms");
    arrayread+=(t2-t1).toInt
    println("读取文件"+arrayread)
    System.out.println("读取最短时间：" + arrayread.min +"ms");
    System.out.println("读取最长时间：" +arrayread.max +"ms");
    System.out.println("读取平均时间：" +arrayread.sum/arrayread.size +"ms");
      **/

/**
    val srcFile: File = new File("D:/regionfs-test/f1.txt")
    val arraywrite= ArrayBuffer[Int]();
    val arrayread = ArrayBuffer[Int]();
    val arrayid = ArrayBuffer[FileId]();

    for (i <-1 to 10){
      val startTime =System.nanoTime()
      client.writeFile(srcFile)
      val endTime=System.nanoTime()
      println("第"+i+"次文件写入时间： "+(endTime-startTime)+"ns");
      arraywrite+=(endTime-startTime).toInt
      val id =Await.result(client.writeFile(srcFile), Duration("10s"))
      arrayid+=id
      //println(id)
    }

    println("写入文件"+arraywrite)
    val mint=(arraywrite.min.toFloat/1000000).formatted("%.2f")
    val maxt=(arraywrite.max.toFloat/1000000).formatted("%.2f")
    val aveint=arraywrite.sum/arraywrite.size
    val avet=(aveint.toFloat/1000000).formatted("%.2f")
    System.out.println("写入最短时间：" + arraywrite.min +"ns"+"----合计约"+mint+"ms");
    System.out.println("写入最长时间：" +arraywrite.max +"ns"+"----合计约"+maxt+"ms");
    System.out.println("写入平均时间：" +arraywrite.sum/arraywrite.size +"ns"+"----合计约"+avet+"ms");
    println(arrayid)
    println("读取文件/-------------------------------------------------------------------------------------------------/")
    for (id <- arrayid){
      val t1=System.currentTimeMillis()
      readFile(id)
      val t2=System.currentTimeMillis()
      //println("第"+i+"次文件读取时间： "+(t2-t1)+"ms");
      arrayread+=(t2-t1).toInt
    }
    println("读取文件"+arrayread)
    System.out.println("读取最短时间：" + arrayread.min +"ms");
    System.out.println("读取最长时间：" +arrayread.max +"ms");
    System.out.println("读取平均时间：" +arrayread.sum/arrayread.size +"ms");
**/
    val sdt="/data/testdata/100MB.txt"
    val srcFile: File = new File(sdt)
    val arraywrite= ArrayBuffer[Int]();
    val arrayread = ArrayBuffer[Int]();
    //val  arrayids =ArrayBuffer[ArrayBuffer[FileId]]();
    val arrayid = ArrayBuffer[FileId]();

    for (i <-1 to 6){

      val startTime =System.currentTimeMillis()
      client.writeFile(srcFile)
      val id =Await.result(client.writeFile(srcFile), Duration("10s"))
      arrayid.+=:(id);

      val endTime=System.currentTimeMillis()
      val costtime=endTime-startTime
      //println(costtime,costtime.toInt)
      println("第"+i+"次文件"+sdt+"写入时间： "+costtime+"ms");
      arraywrite+=costtime.toInt
      //arrayids+=arrayid

    }

    println("写入文件"+arraywrite)

    val avet=arraywrite.sum/arraywrite.size
    val fsum=arraywrite.sum-arraywrite.max-arraywrite.min
    val favet=fsum/(arraywrite.size-2)

    System.out.println("写入最短时间：" + arraywrite.min +"ms"+"舍去");
    System.out.println("写入最长时间：" +arraywrite.max +"ms"+"舍去");
    System.out.println("舍弃写入前平均时间：" +avet +"ms");
    System.out.println("舍弃后写入平均时间：" +favet +"ms");

    println("读取文件/-------------------------------------------------------------------------------------------------/")


//    for (id <- arrayid){
//      val t1=System.currentTimeMillis()
//      readFile(id)
//      val t2=System.currentTimeMillis()
//      arrayread+=(t2-t1).toInt
//    }
    for (i <- 1 to 12){
      val t1=System.currentTimeMillis()
      val fileid=arrayid.apply(0)
      readFile(fileid)
      //println(arrayid.apply(0))
      val t2=System.currentTimeMillis()
      val ct =(t2-t1).toInt
      arrayread+=ct
      println("第"+i+"次文件FileID"+fileid+"读取时间： "+ct+"ms");
    }


    println("读取文件"+arrayread)
    val aver=arrayread.sum/arrayread.size
    val fsumr=arrayread.sum-arrayread.max-arrayread.min
    val faver=fsumr/(arrayread.size-2)
    System.out.println("读取最短时间：" + arrayread.min +"ms");
    System.out.println("读取最长时间：" +arrayread.max +"ms");
    System.out.println("舍弃前读取平均时间：" +aver +"ms");
    System.out.println("舍弃后读取平均时间：" +faver +"ms");


  }



  def writeFile(bytes: Array[Byte]): FileId = {
    Await.result(client.writeFile(ByteBuffer.wrap(bytes)), Duration("10s"))
  }

  def readFile(fileId: org.grapheco.regionfs.FileId):Unit ={
    val futureStr: Future[String] = client.readFile(fileId,(is) => IOUtils.toString(is).asInstanceOf[String])
    val str =Await.result(futureStr,Duration("10s"))
    //println("文件内容为："+str)
  }

 }




