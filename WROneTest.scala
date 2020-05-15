import java.io._
import java.nio.ByteBuffer
import FsClientTest.client
import org.grapheco.regionfs.util.ByteBufferConversions._
import FsClientTest.client
import org.apache.commons.io.IOUtils
import org.grapheco.regionfs.FileId
import org.grapheco.regionfs.client.FsClient

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object WROneTest {

  def main(args:Array[String]):Unit={
    println("Hello, Scala,WROneTest!")
    //System.out.println("准备连接客户端。。。。")
    //val gett01 =System.currentTimeMillis()
    //val client = new FsClient("10.0.85.107:2181")
    val client = new FsClient("10.0.82.216:2181")
    //val gett02 =System.currentTimeMillis()
    System.out.println("已连接上客户端。。。。")
    //System.out.println("连接客户端节耗时"+(gett02-gett01)+"ms")

    //val sdt="/data/testdata/1KB.txt"
    val Array(sdt) = args
    val fsdt="/data1/testdata/"+sdt
    val srcFile: File = new File(fsdt)

    val arraywrite= ArrayBuffer[Int]();
    val arrayread = ArrayBuffer[Int]();
    //val  arrayids =ArrayBuffer[ArrayBuffer[FileId]]();
    val arrayid = ArrayBuffer[FileId]();
    println("写入文件/-----------------"+sdt+"----------------"+1+"个-----------------------------/")
    for (i <-1 to 7){
      val startTime =System.currentTimeMillis()
      //client.writeFile(srcFile)
      val id =Await.result(client.writeFile(srcFile), Duration("1000s"))
      //scala.io.Source.fromFile("/etc/profile").toArray;
      val endTime=System.currentTimeMillis()
      arrayid.+=:(id);
      val costtime=endTime-startTime
      println("第"+i+"次文件"+sdt+"写入时间： "+costtime+"ms");
      arraywrite+=costtime.toInt
      //arrayids+=arrayid
    }
    //println("写入文件"+arraywrite)
    val avet:Float=arraywrite.sum/arraywrite.size
    val fsum=arraywrite.sum-arraywrite.max-arraywrite.min
    val favet:Float=fsum/(arraywrite.size-2)

    System.out.println("最短时间：" + arraywrite.min +"ms--"+"舍去");
    System.out.println("最长时间：" + arraywrite.max+ "ms--"+"舍去");
    System.out.println("写入文件"+sdt+"1个,舍弃前取平均时间：" + avet +"ms"+ "--合计约"+ avet/1000 +"s");
    System.out.println("写入文件"+sdt+"1个,舍弃后取平均时间：" + favet +"ms"+ "--合计约"+ favet/1000 +"s");

    println("读取文件/------------------"+sdt+"-----------------"+1+"个----------------------------------------/")
    val fileid=arrayid.apply(0)
    for (i <- 1 to 7){
      val t1=System.currentTimeMillis()
      val futureStr: Future[String] = client.readFile(fileid,(is) => IOUtils.toString(is).asInstanceOf[String])
      val str =Await.result(futureStr,Duration("1000s"))
      val t2=System.currentTimeMillis()
      val ct =(t2-t1).toInt
      arrayread+=ct
      println("第"+i+"次文件"+sdt+"-"+fileid+"读取时间： "+ct+"ms");
    }

    //println("读取文件"+arrayread)
    val asum:Float=arrayread.sum
    val asize:Float=arrayread.size
    val aver:Float=asum/asize
    val fsumr:Float=arrayread.sum-arrayread.max-arrayread.min
    val faver:Float=fsumr/(arrayread.size-2)

    System.out.println("最短时间：" + arrayread.min +"ms--"+"舍去");
    System.out.println("最长时间：" + arrayread.max+ "ms--"+"舍去");
    System.out.println("读取文件"+sdt+"1个,舍弃前取平均时间：" + aver +"ms"+ "--合计约"+ aver/1000 +"s");
    System.out.println("读取文件"+sdt+"1个,舍弃后取平均时间：" + faver +"ms"+ "--合计约"+ faver/1000 +"s");
    client.close()
    //System.out.println("代码执行结束。。。。。。。。。。。请按Crtl + C停止程序运行！！！")
    //System.out.println("代码执行结束。。。。。。。。。。。")

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

  def toString(filename:String): String = {
    val file: File = new File(filename)
    //val reader:BufferedReader =null
    val sbf: StringBuffer = new StringBuffer()
    val reader: BufferedReader = new BufferedReader(new FileReader(file))
    try {

      val tempStr: String = reader.readLine()
      while ((tempStr) != (null)) {
        sbf.append(tempStr)
      }
      reader.close()
      return sbf.toString()
    } catch {
      case ex: IOException => {
        ex.printStackTrace() // 打印到标准err
        System.err.println("exception===>: ...") // 打印到标准err
      }
    } finally {
      if (reader != null) {
        try {
          reader.close()
        } catch {
          case ex: IOException => {
            ex.printStackTrace() // 打印到标准err
            System.err.println("exception===>: ...") // 打印到标准err
          }
        }
      }
    }
    return sbf.toString()
  }
}
