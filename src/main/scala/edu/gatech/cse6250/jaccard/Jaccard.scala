/**
 *
 * students: please put your implementation in this file!
 */
package edu.gatech.cse6250.jaccard

import edu.gatech.cse6250.model._
import edu.gatech.cse6250.model.{ EdgeProperty, VertexProperty }
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /**
     * Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients.
     * Return a List of top 10 patient IDs ordered by the highest to the lowest similarity.
     * For ties, random order is okay. The given patientID should be excluded from the result.
     */

    /** Remove this placeholder and implement your code */

    val allneighbor = graph.vertices
      .filter(_._2.isInstanceOf[PatientProperty]) // to filter out all non-patient vertice
      .join(graph.collectNeighborIds(EdgeDirection.Out))

    //allneighbor.take(5).foreach(println)

    val neighbor = graph.collectNeighborIds(EdgeDirection.Out)
      .lookup(patientID).flatten.toSet

    //neighbor.take(5).foreach(println)

    val patientjac = allneighbor.map(x => (x._1, jaccard(neighbor, x._2._2.toSet)))

    val topids = patientjac
      .filter(x => x._1 != patientID)
      .takeOrdered(10)(Ordering[Double].reverse.on(x => x._2))
      .map(x => x._1.toLong)
      .toList

    topids
  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
     * Given a patient, med, diag, lab graph, calculate pairwise similarity between all
     * patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where
     * patient-1-id < patient-2-id to avoid duplications
     */

    /** Remove this placeholder and implement your code */

    /**
     * val sc = graph.edges.sparkContext
     * sc.parallelize(Seq((1L, 2L, 0.5d), (1L, 3L, 0.4d)))
     */

    val sc = graph.edges.sparkContext

    val allneighbor = graph.vertices
      .filter(_._2.isInstanceOf[PatientProperty]) // to filter out all non-patient vertice
      .join(graph.collectNeighborIds(EdgeDirection.Out))

    val cartneighbor = allneighbor.cartesian(allneighbor).filter(x => x._1._1 < x._2._1)

    val jacall = cartneighbor
      .map(x => (x._1._1, x._2._1, jaccard(x._1._2._2.toSet, x._2._2._2.toSet)))

    jacall

  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /**
     * Helper function
     *
     * Given two sets, compute its Jaccard similarity and return its result.
     * If the union part is zero, then return 0.
     */

    /** Remove this placeholder and implement your code */

    val jac = if (a.union(b).size == 0) {
      0.0
    } else {
      a.intersect(b).size.toDouble / a.union(b).size.toDouble

    }

    jac
  }

}

