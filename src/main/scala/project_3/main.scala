package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def randomNumber(degree: Int): Int = {
	val random = scala.util.Random
	var num = random.nextInt((2*degree))
	if(num != 2){
		return 1	//0
	} else {
		return -1	//2
	}
  }

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
	/*
    var M = g_in
	var remaining_vertices = g_in.vertices.count
	val random = scala.util.Random
	while (remaining_vertices >= 1) {
		var activeGraph = M.mapVertices((id,_) => randomNumber(4))
		M = activeGraph
		
        for(vertex <- g_in.vertices.collect){
			if(M.subgraph(vpred = (id, attr) => id==vertex._1) != null){
				if(vertex._1==2){
					for(x <- M.collectNeighbors(EdgeDirection.Out).collect){
						//x = neighbor
						if(x._2.length > 0 && x._2(0)._2 == 0){
							println("run")
							println(vertex)
							//println(x)
							var vertex._2 = 1;
							var x._1 = -1;
						}
					}
				}
			}
		}
		M = g_in.subgraph(vpred = (id, attr) => attr == 0 || attr == 2)
		remaining_vertices = M.vertices.count
		println(remaining_vertices)
    }
	*/
	
	val g = Graph.fromEdges[Int, Int](g_in.edges, randomNumber(4), edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
	return g
  }

  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
	for (triplet <- g_in.triplets.collect){
		if (triplet.srcAttr == 1 && triplet.dstAttr == 1){
			return false
		}
	}
	return true
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
