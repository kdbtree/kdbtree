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


package it.uniroma3.dbtree.spi

import it.uniroma3.dbtree.NodeTransfer
import java.io.Serializable

/**
 * Interface for the database connector.
 *
 * This interface provides methods to establish and exploit the connection with the underlying database.
 * The DB-tree class will refer to this interface to create, retrieve, update and delete the persisted nodes necessary
 * to the application logic.
 *
 * All operation modifying the state of the database should be performed within a transaction, in order not to
 * cause inconsistencies.
 *
 * @param K the KeyParser parameter, actually, the key type.
 */
interface Connector <K> where K: Comparable<K>, K: Serializable {

    /**
     * The KeyParser attribute for the Connector.
     *
     * The KeyParser is fundamental to fulfill Connector's responsibilities. In fact, all information related
     * with the keys (parsing, schema, definitions, queries, etc.) are offered by this object.
     */
    val kp : KeyParser<K>

    val connectionUrl: String

    val username: String
    val password: String

    val databaseName: String

    val tableBaseName: String


    // ============== CONNECTION MANAGEMENT =================== //

    /**
     * Connects with the underlying database.
     *
     * The connection should either recover the state left by the previous close(), or (in case no previous
     * state is found) start a new one.
     *
     * Connector should make sure that:
     *  - The connection will be opened on the existing target table, if present, otherwise a new one will be created.
     *  - The target table name should be: "tableName_aggrFunName_kpName" to achieve a unique identification.
     *  - The target table must be provided with the following columns, in order:
     *      - level: of type Int.
     *      - k_min: can span more columns. Its definition is specified by the KeyParser kp.
     *      - k_max: can span more columns. Its definition is specified by the KeyParser kp.
     *      - context: of type String. It will contain the node's serialized elements.
     *  - If a valid target table is found, Connector should ask the client whether he wants to connect or to overwrite
     *  previous data and start a new state (old table is emptied and re-populated).
     *
     *  @param aggrFunName the aggregation function's name.
     *  @param maxLevel the level of the root for the DB-tree.
     *  @param initRootContext the identity aggregate value (for the root) wrt the aggregation function.
     */
    fun connect(aggrFunName: String, maxLevel: Int, initRootContext: String)

    /**
     * Disconnects from the underlying database.
     *
     * Eventual active transactions must be finished.
     * The underlying database must be left in a consistent state, which will be restored by the next connect().
     */
    fun close()

    /**
     * Informs about the state of the current connection.
     *
     * @return true if there is currently no active connection with the database.
     */
    fun isClosed() : Boolean


    // ============== NODES RETRIEVAL =================== //

    /**
     * Returns the DBTree's root node.
     *
     * @return the root NodeTransfer.
     */
    fun getRoot(): NodeTransfer

    /**
     * Retrieve from the database all nodes n s.t. n.k_min < k < n.k_max, ordered by ascending level.
     *
     * @param k the input key.
     * @return the list of retrieved nodes.
     */
    fun getIncludingK(k: K): List<NodeTransfer>

    /**
     * Retrieve from the database the node n s.t. k1 <= n.k_min <= n.k_max <= k2, having maximum level.
     *
     * @param k1 the minimum key in the range.
     * @param k2 the maximum key in the range.
     * @return the list of retrieved nodes.
     */
    fun getRangeQueryNBar(k1: K, k2: K): List<NodeTransfer>

    /**
     * Retrieve from the database all nodes n s.t. n.k_min < k1 < n.k_max ≤ k2, ordered by ascending level.
     *
     * @param k1 the minimum key in the range.
     * @param k2 the maximum key in the range.
     * @return the list of retrieved nodes.
     */
    fun getRangeQueryL(k1: K, k2: K): List<NodeTransfer>

    /**
     * Retrieve from the database all nodes n s.t. k1 ≤ n.k_min < k2 < n.k_max, ordered by ascending level.
     *
     * @param k1 the minimum key in the range.
     * @param k2 the maximum key in the range.
     * @return the list of retrieved nodes.
     */
    fun getRangeQueryR(k1: K, k2: K): List<NodeTransfer>

    /**
     * Retrieve from the database all nodes n s.t. n.min <= k <= n.max, ordered by ascending level.
     *
     * @param k the input key.
     * @return the list of retrieved nodes.
     */
    fun getDelete(k: K): List<NodeTransfer>

    /**
     * Retrieve from the database all nodes n s.t. y1 < n.k_max.y ≤ y2 ∧ (n.k_min.y < y1 ∨ n.k_min.x ≠ n.k_max.x),
     * ordered by ascending level.
     *
     * This method can only be invoked when the key type is composite, i.e. admits a group and a least significant part.
     *
     * @param y1 the list that identify the minimum element in the range.
     * @param y2 the list that identify the maximum element in the range.
     * @return the list of retrieved nodes.
     */
    fun getGroupByL(y1: List<Any>, y2: List<Any>): List<NodeTransfer>

    /**
     * Retrieve from the database all nodes n s.t. y1 ≤ n.k_min.y < y2 ∧ (y2 < n.k_max.y ∨ n.k_min.x ≠ n.k_max.x),
     * ordered by ascending level.
     *
     * This method can only be invoked when the key type is composite, i.e. admits a group and a least significant part.
     *
     * @param y1 the list that identify the minimum element in the range.
     * @param y2 the list that identify the maximum element in the range.
     * @return the list of retrieved nodes.
     */
    fun getGroupByR(y1: List<Any>, y2: List<Any>): List<NodeTransfer>

    /**
     * Retrieve from the database all pairs (x,n) s.t. x ∈ X ∧ n.k_min < (x, y1) < (x, y2) < n.k_max, having maximum
     * level.
     *
     * This method can only be invoked when the key type is composite, i.e. admits a group and a least significant part.
     *
     * @param y1 the list that identify the minimum element in the range.
     * @param y2 the list that identify the maximum element in the range.
     * @return the retrieved pair (x,n).
     */
    fun getGroupByNBar(y1: List<Any>, y2: List<Any>): Map<List<Any>, NodeTransfer>


    // ============== DBTREE MODIFICATIONS =================== //

    /**
     * Insert a batch of new nodes into the database.
     *
     * Should be executed within a transaction.
     *
     * @param nodes the node's list to be created.
     */
    fun create(nodes: Collection<NodeTransfer>)

    /**
     * Insert a node into the database.
     *
     * Should be executed within a transaction.
     *
     * @param node the node to be inserted.
     */
    fun create(node: NodeTransfer)

    /**
     * Delete a batch of nodes from the database.
     *
     * Should be executed within a transaction.
     * The batch of nodes is univocally identified by their primary key, which is represented by the triple:
     * <level, k_min, k_max>. The keys must be passed as serialized.
     *
     * @param nodes the nodes' collection to be deleted.
     */
    fun delete(nodes: Collection<Triple<Int, String, String>>)

    /**
     * Delete node from the database.
     *
     * Should be executed within a transaction.
     * The nodes is univocally identified by its primary key, which is represented by the triple:
     * <level, k_min, k_max>. The keys must be passed as serialized.
     *
     * @param node the node to be deleted.
     */
    fun delete(node: Triple<Int, String, String>)

    /**
     * Update a batch of nodes of the database.
     *
     * Should be executed within a transaction.
     * The batch of nodes will be passed as NodeTransfers instances, so that both (I) their primary keys and (II) the
     * new contexts will be provided.
     *
     * @param nodes the batch to be updated.
     */
    fun update(nodes: Collection<NodeTransfer>)

    /**
     * Update a node of the database.
     *
     * Should be executed within a transaction.
     * The node will be passed as NodeTransfer instance, so that both (I) its primary keys and (II) the
     * new context will be provided.
     *
     * @param node the node to be updated.
     */
    fun update(node: NodeTransfer)

    /**
     * Update the DB-tree persisted state.
     *
     * This method allows to (I) create, (II) update and (III) delete nodes at once from the underlying database.
     * This essentially consists of a combination of other connector's methods, enhancing optimization and
     * transaction control.
     *
     * @param create the batch of nodes to be created.
     * @param update the batch of nodes to be updated.
     * @param delete the batch of nodes to be deleted.
     */
    fun updateDBTree(
        create: Collection<NodeTransfer>,
        update: Collection<NodeTransfer>,
        delete: Collection<Triple<Int, String, String>>)
}