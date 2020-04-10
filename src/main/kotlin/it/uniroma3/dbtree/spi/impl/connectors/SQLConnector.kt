/*
 * Copyright (c) 2020.  Diego Pennino, Maurizio Pizzonia, Alessio Papi
 *
 *     The dbtree library is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     The dbtree library is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with dbtree.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.uniroma3.dbtree.spi.impl.connectors

import it.uniroma3.dbtree.NodeTransfer
import it.uniroma3.dbtree.spi.Connector
import it.uniroma3.dbtree.spi.KeyParser
import it.uniroma3.dbtree.spi.KeyParserGroupBy
import java.io.Serializable
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.util.*
import kotlin.collections.ArrayList

/**
 * [Connector] reference implementation for an underlying SQL database. (Tested on PostgreSQL and mySQL)
 *
 * This class provides methods to establish and exploit the connection with the underlying Postgres database.
 * The DB-tree class will refer to this class to create, retrieve, update and delete the persisted nodes necessary
 * to the application logic.
 *
 * @param K the KeyParser parameter.
 */
class SQLConnector<K>(

    override val connectionUrl: String,
    override val databaseName: String,
    override val username: String,
    override val password: String,
    override val tableBaseName: String,
    override val kp: KeyParser<K>

) : Connector<K> where K: Comparable<K>, K: Serializable
{
    // Connection instance with underlying SQL DBMS
    private lateinit var conn: Connection

    // The full table name obtained as: "$tableBaseName-$aggrFunName"
    private var tableName = ""


    //Functions used to identify the columns k_min and k_max
    private fun max(name:String) = "max_${name}"
    private fun min(name:String) = "min_${name}"

    //We generates the list of the name and definitions of the columns that identify k_max using the keyColumnsDefinitionDB
    private val keyColumnsDefinitionListMax = kp.keyColumnsDefinitionDB.map{ (name,def) -> Pair(max(name), def)}
    //We generates the list of the name and definitions of the columns that identify k_min using the keyColumnsDefinitionDB
    private val keyColumnsDefinitionListMin = kp.keyColumnsDefinitionDB.map{ (name,def) -> Pair(min(name), def)}

    //We obtain the lists of the name of the columns that identify k_max
    private val keyColumnsNameListMax = keyColumnsDefinitionListMax.map{it.first}
    //We obtain the lists of the name of the columns that identify k_min
    private val keyColumnsNameListMin = keyColumnsDefinitionListMin.map{it.first}


    //useful in groupBy functions
    private val mostSignificantPartColumnNameMin = if (kp is KeyParserGroupBy)kp.mostSignificantDefinition.map { min(it.first) } else listOf()
    private val mostSignificantPartColumnNameMax = if (kp is KeyParserGroupBy)kp.mostSignificantDefinition.map { max(it.first) } else listOf()
    private val listSignificantPartColumnNameMin = if (kp is KeyParserGroupBy)kp.leastSignificantDefinition.map { min(it.first) } else listOf()
    private val listSignificantPartColumnNameMax = if (kp is KeyParserGroupBy)kp.leastSignificantDefinition.map { max(it.first) } else listOf()


    private var maxLevel = -1  // reset at each connection

    // ============== CONNECTION MANAGEMENT =================== //

    override fun connect(aggrFunName: String, maxLevel: Int, initRootContext: String){
        tableName = tableBaseName + "_" +
                aggrFunName.toLowerCase() + "_" +
                kp.javaClass.simpleName.toLowerCase()
        this.maxLevel = maxLevel

        connectToDatabase()

        createTable(maxLevel, initRootContext)
    }

    private fun connectToDatabase() {
        val connectionProperties = Properties()
        connectionProperties.put("user",username)
        connectionProperties.put("password",password)
        conn = DriverManager.getConnection(connectionUrl,connectionProperties)
        try{
            val st = conn.createStatement()
            st.execute("CREATE DATABASE ${databaseName}")
            st.close()
        }catch (e: SQLException){
            println("Database ${databaseName} is Present")
        }
        conn.close()
        conn = DriverManager.getConnection(connectionUrl+databaseName,connectionProperties)
    }



    private fun createTable(maxLevel: Int, initRootContext: String) {
        val keyColumnsDefinition = keyColumnsDefinitionListMin.plus(keyColumnsDefinitionListMax).map { "${it.first} ${it.second}" }.joinToString(", ")
        val keyColumnsName = keyColumnsNameListMin.plus(keyColumnsNameListMax).joinToString(", ")

        val st= conn.createStatement()
        st.execute("CREATE TABLE IF NOT EXISTS $tableName (" +
                "level INTEGER NOT NULL, " +
                "$keyColumnsDefinition, " +
                "context text, " +
                "PRIMARY KEY ($keyColumnsName, level));")
        val top = st.executeQuery("SELECT * FROM $tableName WHERE level=$maxLevel")
        if(!top.next()) {
            st.execute("INSERT INTO $tableName VALUES("+
                    "$maxLevel, " +
                    "${kp.toDB(kp.getMin())}, " +
                    "${kp.toDB(kp.getMax())}, " +
                    "'$initRootContext'"+
                    ") ")
        }
        st.close()
    }

    private fun deleteTable(){
        val sql = "DROP TABLE $tableName;"

        val stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    }

    override fun close() {
        conn.close()
        maxLevel = -1
    }

    override fun isClosed() : Boolean {
        return conn.isClosed
    }


    // ============== NODES RETRIEVAL =================== //

    override fun getRoot(): NodeTransfer {
        val query = "" +
                "SELECT * " +
                "FROM $tableName " +
                "WHERE level = $maxLevel;"

        val results = executeQuery(query)

        if (results.size > 1)
            throw Exception("Multiple root nodes were found.")

        return results[0]
    }

    override fun getIncludingK(k: K): List<NodeTransfer> {

        val query = "" +
                "SELECT * " +
                "FROM $tableName " +
                "WHERE (${kp.ltDB(keyColumnsNameListMin, k)}) " +
                "AND (${kp.gtDB(keyColumnsNameListMax, k)}) " +
                "ORDER BY level;"

        return executeQuery(query)
    }

    override fun getDelete(k: K): List<NodeTransfer> {

        val query = "" +
                "SELECT * " +
                "FROM $tableName " +
                "WHERE (${kp.leqDB(keyColumnsNameListMin,k)}) " +
                "AND (${kp.geqDB(keyColumnsNameListMax,k)}) " +
                "ORDER BY level;"

        return executeQuery(query)
    }

    override fun getRangeQueryNBar(k1: K, k2: K): List<NodeTransfer> {

        val query = "" +
                "SELECT * " +
                "FROM $tableName " +
                "WHERE ${kp.ltDB(keyColumnsNameListMin,k1)} " +
                "AND ${kp.gtDB(keyColumnsNameListMax,k2)} " +
                "ORDER BY level LIMIT 1"

        return executeQuery(query)
    }



    override fun getRangeQueryL(k1: K, k2: K): List<NodeTransfer> {

        val query = "" +
                "SELECT * " +
                "FROM $tableName " +
                "WHERE ${kp.ltDB(keyColumnsNameListMin,k1)} " +
                "AND " + kp.gtDB(keyColumnsNameListMax,k1) + " " +
                "AND " + kp.leqDB(keyColumnsNameListMax,k2) + " " +
                "ORDER BY level"

        return executeQuery(query)
    }

    override fun getRangeQueryR(k1: K, k2: K): List<NodeTransfer> {

        val query = "" +
                "SELECT * " +
                "FROM $tableName " +
                "WHERE ${kp.geqDB(keyColumnsNameListMin,k1)}  " +
                "AND ${kp.gtDB(keyColumnsNameListMax, k2)} " +
                "AND ${kp.ltDB(keyColumnsNameListMin,k2)} " +
                "ORDER BY level"

        return executeQuery(query)
    }

    override fun getGroupByL(y1: List<Any>, y2: List<Any>): List<NodeTransfer> {

        kp as KeyParserGroupBy

        val query = "SELECT * " +
                "FROM $tableName " +
                "WHERE ( ${kp.lsp_gtDB(listSignificantPartColumnNameMax,y1)} "+
                "AND ${kp.lsp_leqDB(listSignificantPartColumnNameMax, y2)}) " +
                "AND (${kp.lsp_ltDB(listSignificantPartColumnNameMin,y1)} " +
                "OR ${kp.most_neqDB(mostSignificantPartColumnNameMin,mostSignificantPartColumnNameMax)}) " +
                "ORDER BY level"

        return executeQuery(query)
    }

    override fun getGroupByR(y1: List<Any>, y2: List<Any>): List<NodeTransfer> {
        kp as KeyParserGroupBy

        val query = "" +
                "SELECT * " +
                "FROM $tableName " +
                "WHERE ( ${kp.lsp_geqDB(listSignificantPartColumnNameMin,y1)} " +
                "AND ${kp.lsp_ltDB(listSignificantPartColumnNameMin,y2)}) " +
                "AND (${kp.lsp_gtDB(listSignificantPartColumnNameMax,y2)} " +
                "OR ${kp.most_neqDB(mostSignificantPartColumnNameMin,mostSignificantPartColumnNameMax)}) " +
                "ORDER BY level"

        return executeQuery(query)
    }

    override fun getGroupByNBar(y1: List<Any>, y2: List<Any>): Map<List<Any>, NodeTransfer> {
        kp as KeyParserGroupBy

        val query = "" +
                "(SELECT * " +
                "FROM $tableName " +
                "WHERE ${kp.lsp_ltDB(listSignificantPartColumnNameMin,y1)} " +
                "AND ${kp.lsp_gtDB(listSignificantPartColumnNameMax,y2)}) " +
                "UNION (SELECT * " +
                "FROM $tableName " +
                "WHERE ${kp.most_neqDB(mostSignificantPartColumnNameMin,mostSignificantPartColumnNameMax)}) " +
                "ORDER BY level;"

        val temp = executeQuery(query)

        return auxGroupByNBar(temp, y1, y2)
    }

    /**
     * Auxiliary function for [getGroupByNBar].
     *
     * Extracts from [temp] the correct list of NBar(s) mapped to their corresponding group.
     *
     * @param temp the extended list of NBar(s).
     * @param y1 start of the queried range.
     * @param y2 end of the queried range.
     * @return the map <group, corresponding_NBar>.
     */
    private fun auxGroupByNBar(temp: List<NodeTransfer>, y1: List<Any>, y2: List<Any>): MutableMap<List<Any>, NodeTransfer> {
        kp as KeyParserGroupBy

        val res = mutableMapOf<List<Any>, NodeTransfer>()
        val assignedGroups = ArrayList<Any>()

        for (n in temp){  // for each node
            for (g in findGroups(n)){  // query its elements' groups list
                if (!assignedGroups.contains(g) &&
                        kp.parse(n.k_min) < kp.bind(g,y1) &&
                        kp.bind(g,y2) < kp.parse(n.k_max)){
                    assignedGroups.add(g)
                    res[g] = n
                }
            }
        }
        return res
    }

    /**
     * Find the groups spanned by the elements of a node [n].
     *
     * @param n the node to be checked.
     * @return the groups spanned by its elements.
     */
    private fun findGroups(n: NodeTransfer): Set<List<Any>> {
        kp as KeyParserGroupBy
        val groups = HashSet<List<Any>>()
        for (i in n.parseContext())
            if (i.isPair())
                groups.add(kp.group(i.k()))
        return groups
    }

    /**
     * Auxiliary function that actually retrieves the query nodes.
     *
     * In executeQuery, the library connects with the underlying Postgres DBMS and actually executes the given query.
     * Any SQL Exception is treated aborting the connection and throwing to the DBTree a generic Connector Exception.
     *
     * @param query the query to execute.
     * @return the target list of nodes as NodeTransfer instances.
     */
    private fun executeQuery(query: String) : List<NodeTransfer> {
        val nodes = ArrayList<NodeTransfer>()  // the list of nodes to be returned

        val stmt = conn.createStatement()
        val resultSet = stmt.executeQuery(query)

        while (resultSet.next())
            nodes.add(kp.toNodeTransfer(resultSet))

        resultSet.close()
        stmt.close()


        return nodes
    }


    // ============== DBTREE MODIFICATIONS =================== //

    override fun create(nodes: Collection<NodeTransfer>) {

        val st = conn.createStatement()
        for (n in nodes) {
            st.addBatch(
                "INSERT INTO $tableName VALUES (" +
                        "${n.level}, " +
                        "${kp.toDB(kp.parse(n.k_min))}, " +
                        "${kp.toDB(kp.parse(n.k_max))}, " +
                        "'${n.context}');" )
        }

        transaction(st)

    }

    override fun create(node: NodeTransfer) {

        val st = conn.createStatement()
        st.addBatch(
            "INSERT INTO $tableName VALUES (" +
                    "${node.level}, " +
                    "${kp.toDB(kp.parse(node.k_min))}, " +
                    "${kp.toDB(kp.parse(node.k_max))}, " +
                    "'${node.context}');" )
        transaction(st)
    }

    override fun delete(nodes: Collection<Triple<Int, String, String>>) {
        val st: Statement

        st = conn.createStatement()

        for (n in nodes) {
            st.addBatch(
                "DELETE FROM $tableName " +
                        "WHERE level = ${n.first} " +
                        "AND ${kp.eqDB(keyColumnsNameListMin, kp.parse(n.second))} " +
                        "AND ${kp.eqDB(keyColumnsNameListMax, kp.parse(n.third))};" )
        }

        transaction(st)
    }

    override fun delete(node: Triple<Int, String, String>) {

        val st = conn.createStatement()

        st.addBatch(
            "DELETE FROM $tableName " +
                    "WHERE level = ${node.first} " +
                    "AND ${kp.eqDB(keyColumnsNameListMin, kp.parse(node.second))} " +
                    "AND ${kp.eqDB(keyColumnsNameListMax, kp.parse(node.third))};" )

        transaction(st)
    }

    override fun update(nodes: Collection<NodeTransfer>) {

        val st = conn.createStatement()

        for (n in nodes) {
            st.addBatch(
                "UPDATE $tableName SET context = '${n.context}' " +
                        "WHERE level = ${n.level} " +
                        "AND ${kp.eqDB(keyColumnsNameListMin, kp.parse(n.k_min))} " +
                        "AND ${kp.eqDB(keyColumnsNameListMax, kp.parse(n.k_max))};" )
        }

        transaction(st)
    }

    override fun update(node: NodeTransfer) {
        val st = conn.createStatement()
        st.addBatch(
            "UPDATE $tableName SET context = '${node.context}' " +
                    "WHERE level = ${node.level} " +
                    "AND ${kp.eqDB(keyColumnsNameListMin, kp.parse(node.k_min))} " +
                    "AND ${kp.eqDB(keyColumnsNameListMax, kp.parse(node.k_max))};" )

        transaction(st)

    }

    override fun updateDBTree(
        create: Collection<NodeTransfer>,
        update: Collection<NodeTransfer>,
        delete: Collection<Triple<Int, String, String>>
    ) {
        val st = conn.createStatement()

        create.forEach {
            st.addBatch("INSERT INTO $tableName VALUES (" +
                    "${it.level}, " +
                    "${kp.toDB(kp.parse(it.k_min))}, " +
                    "${kp.toDB(kp.parse(it.k_max))}, " +
                    "'${it.context}');" )
        }
        update.forEach {
            st.addBatch("UPDATE $tableName SET " +
                    "CONTEXT = '${it.context}' " +
                    "WHERE level = ${it.level} " +
                    "AND ${kp.eqDB(keyColumnsNameListMin,kp.parse(it.k_min))} " +
                    "AND ${kp.eqDB(keyColumnsNameListMax,kp.parse(it.k_max))};" )
        }
        delete.forEach {
            st.addBatch("DELETE FROM $tableName " +
                    "WHERE level = ${it.first} " +
                    "AND ${kp.eqDB(keyColumnsNameListMin, kp.parse(it.second))} " +
                    "AND ${kp.eqDB(keyColumnsNameListMax, kp.parse(it.third))};" )
        }

        transaction(st)

    }

    /**
     * Method to perform a batch of operations within a transaction.
     *
     * @param st the open statement, containing a batch of operations ready to be executed.
     *
     */
    private fun transaction(st: Statement) {
        conn.autoCommit = false  // switch to manual commit mode

        st.executeBatch()
        conn.commit()
        st.close()
        conn.autoCommit = true  // restore the automatic commit mode

    }

}
