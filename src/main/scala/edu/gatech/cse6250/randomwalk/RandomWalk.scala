package edu.gatech.cse6250.randomwalk

import edu.gatech.cse6250.model.{ PatientProperty, EdgeProperty, VertexProperty }
import org.apache.spark.graphx._

object RandomWalk {

  def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long, numIter: Int = 100, alpha: Double = 0.15): List[Long] = {
    /**
     * Given a patient ID, compute the random walk probability w.r.t. to all other patients.
     * Return a List of patient IDs ordered by the highest to the lowest similarity.
     * For ties, random order is okay
     */

    /** Remove this placeholder and implement your code */

    val personalized = patientID
    val src: VertexId = patientID

    var rankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices { (id, attr) =>
        if (!(id != src)) alpha else 0.0
      }

    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      prevRankGraph = rankGraph

      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => if (id == patientID) alpha + (1.0 - alpha) * msgSum else (1.0 - alpha) * msgSum
      }

      rankGraph.edges.foreachPartition(x => {})

      prevRankGraph.vertices.unpersist(false)

      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    /**
     * var patient_vertices = graph.vertices.filter(_._2.isInstanceOf[PatientProperty]).filter(_._1 != patientID).map(x => x._1).collect.toList
     *
     * val top10 = rankGraph.vertices.filter(f => patient_vertices.contains(f._1)).takeOrdered(10)(Ordering[Double].reverse.on(x => x._2)).map(_._1)
     *
     * top10.toList
     */

    val plist = graph.vertices.filter(x => x._2.isInstanceOf[PatientProperty])

    val plist2 = plist.filter(x => x._1 != patientID).map(x => x._1).collect.toList

    val allpat = rankGraph.vertices
      .filter(x => plist2.contains(x._1))

    val top = allpat
      .takeOrdered(10)(Ordering[Double].reverse.on(x => x._2)).map(_._1).toList

    top

  }
}
