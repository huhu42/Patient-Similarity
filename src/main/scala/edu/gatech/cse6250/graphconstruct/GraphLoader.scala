/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse6250.graphconstruct

import edu.gatech.cse6250.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphLoader {
  /**
   * Generate Bipartite Graph using RDDs
   *
   * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
   * @return: Constructed Graph
   *
   */
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
    medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    /** HINT: See Example of Making Patient Vertices Below */

    val vertexPatient: RDD[(VertexId, VertexProperty)] = patients
      .map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))

    val mPatient = patients.map(patient => patient.patientID.toLong).max()

    val dstartIndex = mPatient + 1

    //println(dstartIndex)
    //println(vertexPatient.count())

    val diagnosticVertexIdRDD = diagnostics.
      map(_.icd9code).
      distinct.
      zipWithIndex.
      map {
        case (icd9code, zeroBasedIndex) =>
          (icd9code, zeroBasedIndex + dstartIndex)
      }

    val diagnostic2VertexId = diagnosticVertexIdRDD.collect.toMap

    val diagnosticVertex = diagnosticVertexIdRDD.
      map { case (icd9code, index) => (index, DiagnosticProperty(icd9code)) }.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]

    //println(diagnosticVertex.count())

    //diagnosticVertex.foreach(println)

    val lstartIndex = diagnostic2VertexId.size + dstartIndex

    val labresultVertexIdRDD = labResults.
      map(_.labName).
      distinct.
      zipWithIndex.
      map {
        case (testname, zeroBasedIndex) =>
          (testname, zeroBasedIndex + lstartIndex)
      }

    val lab2VertexId = labresultVertexIdRDD.collect.toMap

    val LabResultVertex = labresultVertexIdRDD.
      map { case (test, index) => (index, LabResultProperty(test)) }.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]

    //println(lstartIndex)

    //LabResultVertex.foreach(println)

    //println(LabResultVertex.count())

    val mstartIndex = lab2VertexId.size + lstartIndex

    //println(mstartIndex)

    val medicationVertexIdRDD = medications.
      map(_.medicine).
      distinct.
      zipWithIndex.
      map {
        case (med, zeroBasedIndex) =>
          (med, zeroBasedIndex + mstartIndex)
      }

    val med2VertexId = medicationVertexIdRDD.collect.toMap

    val medicationVertex = medicationVertexIdRDD.
      map { case (med, index) => (index, MedicationProperty(med)) }.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]

    //medicationVertex.foreach(println)

    println(medicationVertex.count())

    val vertices = vertexPatient.union(medicationVertex).union(LabResultVertex).union(diagnosticVertex)

    println(vertices.count())

    val edgePatientLab = labResults
      .map(x => ((x.patientID, x.labName), x))
      .groupByKey()
      .map(x => ((x._1._1, x._1._2), x._2.last))

    edgePatientLab.take(5).foreach(println)

    val edgePatientLab1: RDD[Edge[EdgeProperty]] = edgePatientLab
      .map(x => Edge(x._1._1.toLong, lab2VertexId(x._1._2), PatientLabEdgeProperty(x._2)))

    val edgePatientLabbi =
      edgePatientLab1.union(edgePatientLab1.map(x => Edge(x.dstId, x.srcId, x.attr)))

    val edgePatientMedication = medications
      .map(x => ((x.patientID, x.medicine), x))
      .groupByKey()
      .map(x => ((x._1._1, x._1._2), x._2.last))

    val edgePatientMedication1: RDD[Edge[EdgeProperty]] = edgePatientMedication
      .map(x => Edge(x._1._1.toLong, med2VertexId(x._1._2), PatientMedicationEdgeProperty(x._2)))

    val edgePatientMedicationbi =
      edgePatientMedication1.union(edgePatientMedication1.map(x => Edge(x.dstId, x.srcId, x.attr)))

    val edgePatientDiagnostic = diagnostics
      .map(x => ((x.patientID, x.icd9code), x))
      .groupByKey()
      .map(x => ((x._1._1, x._1._2), x._2.last))

    val edgePatientDiagnostic1: RDD[Edge[EdgeProperty]] = edgePatientDiagnostic
      .map(x =>
        Edge(x._1._1.toLong, diagnostic2VertexId(x._1._2), PatientDiagnosticEdgeProperty(x._2)))

    val edgePatientDiagnosticbi = edgePatientDiagnostic1.union(edgePatientDiagnostic1.map(x => Edge(x.dstId, x.srcId, x.attr)))

    val edges = edgePatientLabbi.union(edgePatientMedicationbi).union(edgePatientDiagnosticbi)

    val graph: Graph[VertexProperty, EdgeProperty] = Graph[VertexProperty, EdgeProperty](vertices, edges)

    graph
  }
}
