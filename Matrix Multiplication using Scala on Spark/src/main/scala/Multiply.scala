
import org.apache.spark.{SparkConf, SparkContext}
import java.io.PrintWriter

object Multiply {
  def main(args: Array[String]): Unit= {

  //  System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf()
    //conf.setMaster("local")
    conf.setAppName("First Application")
  //  conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val matrix_m = sc.textFile(args(0))
      val matrix_m_list=matrix_m.map(l => {
      val a = l.split(",")
      (a(0).toInt, a(1).toInt, a(2).toDouble)
    })
    val matrix_m_map= matrix_m_list.map( matrix_m_list => (matrix_m_list._2, matrix_m_list))
   // println("--Print matrix m--starts --")
  //  matrix_m.foreach(println)
  //  matrix_m_list.foreach(println)
   // matrix_m_map.foreach(println)
  //  println("--Print matrix m--ends --")
  //  println("Reached start")

    val matrix_n = sc.textFile(args(1))
    val matrix_n_list=matrix_n.map(l => {
      val a = l.split(",")
      (a(0).toInt, a(1).toInt, a(2).toDouble)
    } )
    val matrix_n_map =matrix_n_list.map( matrix_n_list => (matrix_n_list._1,matrix_n_list))
  //  println("--Print matrix n--starts --")
  //  matrix_n.foreach(println)
  //  matrix_n_list.foreach(println)
  //  matrix_n_map.foreach(println)
  //  println("--Print matrix n--ends --")


  //  println(matrix_n.toString())
//    val multiply = matrix_m_map.join(matrix_n_map)
//      .map{ case (k, (matrix_m_list,matrix_n_list)) =>
//        ((matrix_m_list._1,matrix_n_list._2),(matrix_m_list._3 * matrix_n_list._3)) }
    val joined_product=matrix_m_map.join(matrix_n_map)

 //  println("joined_product starts")
 //  joined_product.foreach(println)
  //  println("joined_product ends")
    val multiply = joined_product
          .map{ case (k, (matrix_m_list,matrix_n_list)) =>
            ((matrix_m_list._1,matrix_n_list._2),(matrix_m_list._3 * matrix_n_list._3)) }

   // println("multiply starts")
   // multiply.foreach(println )
   // println("multiply starts")

    val finalValues = multiply.reduceByKey((x,y) => (x+y))


    //val sortedValues = finalValues.sortByKey(true, 0)

    //val finalResult = sortedValues.collect()

  //  finalResult.foreach(println)
    var output_text=""
//    for (i <- 0 until finalResult.size-1 ; j <- 0 until finalResult.size-1)
//    {
//      output_text= output_text + finalResult(i)(j)


   //   for (i <- 0 until finalResult.size )
     // {
       // var temp = finalResult(i)
        //output_text= output_text + finalResult(i)(j)
      //  temp match {case ((a,b),c) => output_text=output_text+a+" "+b+" "+c}
      //  output_text=output_text+"\n"
       // output_text = output_text + temp._1._1 +" "+temp._1._2+" "+temp._2+"\n"
//        temp match {case ((x,_),_) => output_text=output_text+x}
//        temp match {case ((_,x),_) => output_text=output_text+x}
//        temp match {case ((_,_),x) => output_text=output_text+x}

     // }
    //println(output_text)
   //val output=sc.parallelize(output_text)
    //output_text.saveAsTextFile("solution5.txt")

//    val output = fs.create(new Pa("sample.txt"))
//    val writer = new PrintWriter(output)
//    writer.write(output_text)
   // val output =sc.parallelize(finalResult)
    finalValues.saveAsTextFile(args(2))
    //new PrintWriter(new Path(args(2))) { write(output_text); close }

    sc.stop()
    //  println("Reached here")
   // val matrix_n = sc.textFile("N-matrix-small.txt")
  //  println(matrix_n)
  }
}
