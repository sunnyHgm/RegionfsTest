import java.io.{File, FileInputStream, InputStream}
import java.nio.ByteBuffer

import FsClientTest.client

import org.apache.commons.io.IOUtils
import org.grapheco.regionfs.FileId
import org.grapheco.regionfs.client.FsClient

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object WRMoreTest {

  def main(args:Array[String])  {
    println("Hello, Scala!")

    //val client = new FsClient("10.0.85.107:2181")
    System.out.println("准备连接客户端。。。。")
    val gett01 =System.currentTimeMillis()
    val client = new FsClient("10.0.85.107:2181")
    val gett02 =System.currentTimeMillis()
    System.out.println("已连接上客户端。。。。")
    System.out.println("连接客户端节耗时"+(gett02-gett01)+"ms")

    //val sdt="/data/testdata/1KB.txt"
    val Array(sdt,count) = args
    val fsdt="/data/testdata/"+sdt
    //val srcFile: File = new File(sdt)

    val inputStream = new FileInputStream(fsdt)


    System.out.println("从文件开始获取字节。。。。")
    val gett1 =System.currentTimeMillis()
    val bytes=inputStreamToByteArray(inputStream)
    val gett2 =System.currentTimeMillis()
    System.out.println("从文件获取字节结束。。。。")
    System.out.println("从文件获取字节耗时"+(gett2-gett1)+"ms")



    //val bytes=srcFile.toString.getBytes
    val arraywrite= ArrayBuffer[Int]();
    val arrayread = ArrayBuffer[Int]();
    val arrayids =ArrayBuffer[ArrayBuffer[FileId]]();

    println("写入文件/-------------------"+sdt+"-----------------------"+count.toInt+"个---------------------------------------------/")
    for (i <- 1 to 12){
      val arrayid = ArrayBuffer[FileId]();
      val startTime =System.currentTimeMillis()
      for (i <- 1 to count.toInt){
        writeFile(bytes)
        val id =writeFile(bytes)
        arrayid.+=:(id);

      }
      val endTime=System.currentTimeMillis()

      val costtime=endTime-startTime
      //println(costtime,costtime.toInt)
      println("第"+i+"次"+count.toInt+"个文件"+sdt+"写入时间： "+costtime+"ms");
      arraywrite+=costtime.toInt
      arrayids+=arrayid

    }

    //println("写入文件"+arraywrite)

    val avet:Float=arraywrite.sum/arraywrite.size
    val fsum=arraywrite.sum-arraywrite.max-arraywrite.min
    val favet:Float=fsum/(arraywrite.size-2)


    System.out.println("最短时间：" + arraywrite.min +"ms--"+"舍去");
    System.out.println("最长时间：" + arraywrite.max+ "ms--"+"舍去");
    System.out.println("写入文件"+sdt+count.toInt+"个,舍弃前取平均时间：" + avet +"ms"+ "合计约"+ avet/1000 +"s");
    System.out.println("写入文件"+sdt+count.toInt+"个,舍弃后取平均时间：" + favet +"ms"+ "合计约"+ favet/1000 +"s");

    println("读取文件/-------------------"+sdt+"-----------------------"+count.toInt+"个---------------------------------------------/")


    val arrayid0=arrayids.apply(0)
    System.out.println("读取文件fileID列表："+arrayid0+"列表长度为："+arrayid0.size)
    for (i <- 1 to 12){

      val t1=System.currentTimeMillis()

      for (id <- arrayid0){
          readFile(id)
      }

      val t2=System.currentTimeMillis()
        //println("第"+i+"次文件读取时间： "+(t2-t1)+"ms");
      val ct=(t2-t1).toInt
      arrayread+=ct
      println("第"+i+"次"+count.toInt+"个文件"+sdt+"读取时间： "+(t2-t1).toInt+"ms");
    }

    //println("读取文件"+arrayread)
    val aver:Float=arrayread.sum/arrayread.size
    val fsumr=arrayread.sum-arrayread.max-arrayread.min
    val faver:Float=fsumr/(arrayread.size-2)


    System.out.println("最短时间：" + arrayread.min +"ms--"+"舍去");
    System.out.println("最长时间：" + arrayread.max+ "ms--"+"舍去");
    System.out.println("读取文件"+sdt+count.toInt+"个,舍弃前取平均时间：" + aver +"ms"+ "合计约"+ aver/1000 +"s");
    System.out.println("读取文件"+sdt+count.toInt+"个,舍弃后取平均时间：" + faver +"ms"+ "合计约"+ faver/1000 +"s");
    client.close()
    System.out.println("代码执行结束。。。。。。。。。。。请按Crtl + C停止程序运行！！！")
    System.out.println("代码执行结束。。。。。。。。。。。请按Crtl + C停止程序运行！！！")

  }

  /**
    * inputStream to Array[Byte] method
    **/
  def inputStreamToByteArray(is: InputStream): Array[Byte] = {
    val buf = ListBuffer[Byte]()
    var b = is.read()
    while (b != -1) {
      buf.append(b.byteValue)
      b = is.read()
    }
    buf.toArray
    // Resource.fromInputStream(in).byteArray
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
