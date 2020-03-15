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

package it.uniroma3.dbtree


import kotlinx.coroutines.*
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.io.Serializable
import java.nio.Buffer
import java.util.*
import kotlin.collections.ArrayList


/**
 * The DBTree class.
 *
 * @param K the key for the DB nodes.
 * @param V the values associated with the keys.
 * @param A the partial result of the aggregation function.
 * @param D the definitive result of the aggregation function.
 * @property f the aggregation function of the DB-tree.
 * @property c the connector of the DB-tree.
 */


class DBTree<K, V: Serializable, A: Serializable, D> (
    private val f: Function<V, A, D>,
    private val c: Connector<K>
) where K: Comparable<K>, K: Serializable
{


    /**
     * The maximum level for the DB-tree.
     *
     * Here is where the root node will be positioned when the table is first initialized.
     */
    private val maxLevel = Int.MAX_VALUE

    /**
     * The key Parser for the DB-tree.
     */
    private val kp = c.kp


    init {
        val initialRootContext = "A:${f.identity()}"
        val aggrFunName = f.javaClass.simpleName
        c.connect(aggrFunName, maxLevel, initialRootContext)
    }

    /*private fun finalize(){
        c.close()
    }*/

    fun close() = c.close()


    // ============== SERVICES =================== //

    /**
     * Extraction of a random level for the insertion of a new key.
     *
     * The extraction uses the new key as seed, in order to provide the DB-tree with a "deterministically random"
     * structure, hence allowing the library's coverage over non-associative functions.
     *
     * @param k the string of the new key.
     * @return the corresponding extracted level.
     */
    private fun extractLevel(k : String): Int {
        val md = MessageDigest.getInstance("SHA-1")
        val hash = md.digest(k.toByteArray(StandardCharsets.UTF_8))
        var buffer = ByteBuffer.allocate(hash.size)
        buffer.put(hash)
        return java.lang.Long.numberOfLeadingZeros(((buffer as Buffer).flip() as ByteBuffer).long)
    }

    /**
     * Algorithm for performing an aggregate range query on the DB-tree.
     *
     * The algorithm accepts as input the range <k',k">.
     * Neither k' nor k" must be explicitly contained in the DB-tree T.
     * The algorithm computes the aggregate value for the values of all keys between k' and k" in T.
     *
     * @param range the input range.
     * @return the aggregate result of the range query.
     */
    fun rangeQuery(range: Pair<K, K>) : D? {
        val k1 = range.first
        val k2 = range.second

        if (k1 == kp.getMin() && k2 == kp.getMax()) {
            val a = Node(c.getRoot()).context.first.v()
            return f.h(f.parseA(a))
        }
        lateinit var L: List<Node>
        lateinit var R: List<Node>
        var n_bar: Node? = null

        val threads : MutableList<Thread> = mutableListOf()
        threads.add( Thread{ L = c.getRangeQueryL(k1,k2).map { Node(it) } }.apply{start()} )
        threads.add( Thread{ R = c.getRangeQueryR(k1,k2).map { Node(it) } }.apply{start()} )
        threads.add( Thread{ n_bar = c.getRangeQueryNBar(k1,k2).map { Node(it) }.minBy { it.level } }.apply{start()} )
        threads.forEach { it.join() }

        return calculateAggr(L, R, n_bar, k1, k2)
    }

    /**
     * Auxiliary function for the get algorithm.
     *
     * This function accepts as input the DB-tree T list of nodes interested by an aggregate range query and performs
     * all the in-memory computation of the resulting aggregated value.
     *
     * @param L the sequence of nodes resulting from the selection from T of all nodes n such that
     * n.min < k' < n.max ≤ k", ordered by ascending level.
     * @param R the sequence of nodes resulting from the selection from T of all nodes n such that
     * k' ≤ n.min < k" < n.max, ordered by ascending level.
     * @param n_bar the node n in T such that n.min < k' < k" < n.max, with minimum level.
     * @param k1 the first key of the range (k').
     * @param k2 the last key of the range (k").
     * @return the aggregate result of the range query.
     */
    private fun calculateAggr(L:List<Node>, R:List<Node>, n_bar: Node?, k1: K, k2: K) : D? {
        var a_l = f.identity()  // left aggregate value
        var a_r = f.identity()  // right aggregate value
        var e_k: K  // key iterator
        val aggrList = ArrayList<A?>()

        L.forEach { node ->
            aggrList.add(a_l)
            e_k = node.k_min  // set key iterator
            node.forEach { elem ->
                if (elem.isPair())
                    e_k = kp.parse(elem.k())
                if (e_k >= k1 )
                    aggrList.add(
                        if (elem.isAgg()) f.parseA(elem.v())
                        else f.g(f.parseV(elem.v())) )
            }
            a_l = f.f(aggrList)
            aggrList.clear()
        }

        R.forEach { node ->
            val nodeIterator = node.iterator()  // node iterator
            e_k = node.k_min  // set key iterator
            aggrList.add(a_r)

            while (nodeIterator.hasNext()) {
                val e = nodeIterator.next()  // consider next element
                // if it's a pair, recalculate the scrolling key
                if (e.isPair() && kp.parse(e.k()) <= k2)
                    aggrList.add(f.g(f.parseV(e.v())))
                else {
                    val tmp = nodeIterator.nextPair()

                    if (tmp != null && kp.parse(tmp.k()) <= k2)  // case e is agg and it's in range
                        aggrList.add(f.parseA(e.v()))
                    else  // case e is a pair/agg out of range
                        break
                }
            }
            a_r = f.f(aggrList)
            aggrList.clear()
        }

        aggrList.add(a_l)

        // step 15
        val n_it = n_bar!!.iterator()
        e_k = n_bar.k_min

        while (n_it.hasNext()) {
            val e = n_it.next()  // consider next element
            if (e.isPair()) {
                if (kp.parse(e.k()) in k1..k2)  // element in range
                    aggrList.add(f.g(f.parseV(e.v())))
            }
            else {
                val previous = n_it.prevPair()
                val next = n_it.nextPair()

                if ((previous != null && next != null) &&
                    kp.parse(next.k()) <= k2 && kp.parse(previous.k())>= k1)
                    aggrList.add(f.parseA(e.v()))
            }
        }
        aggrList.add(a_r)

        return f.h(f.f(aggrList))
    }

    /**
     * This algorithm propagates the change of values of a certain node into the aggregate values in all its ancestors.
     * It also inserts aggregate values that should be present in the aggregate sequence since the corresponding child
     * is present.
     *
     * The sequence U of nodes, in ascending order of level, representing an ascending path in a DB-tree.
     * The first node of U is denoted n_bar, and contains the last modified node.
     *
     * @param U the ascending path to be corrected.
     */
    private fun updateAggr(U: List<Node>){

        val n_bar = U[0]  // last modified node

        // If n_bar is the top level, it means the last dbtree's node has been deleted.
        // In this case, the method only has to reset its aggregate value, than returns.
        if(n_bar.isTop()){
            n_bar.setFirstA(f.identity())
            return
        }

        var a = n_bar.getA()  // aggregate value of n_bar
        val k = n_bar.firstKey()  // any n_bar's key

        // After removing n_bar from U, do step 3
        U.drop(1).forEach {node ->

            if(node.isTop())
                node.setFirstA(a)

            else {
                if (k < node.firstKey())
                    node.setFirstA(a)

                else if (k > node.lastKey())
                    node.setLastA(a)

                else {
                    val nodeIterator = node.iterator()
                    var updated = false
                    while (nodeIterator.hasNext() && !updated) {
                        var e = nodeIterator.next()  // consider next element

                        if (e.isPair()) {
                            val k1 = kp.parse(e.k())
                            val k2 = kp.parse(nodeIterator.nextPair()!!.k())  // surely not null (wouldn't enter the else otherwise)

                            if (k1 < k && k < k2) {
                                val pos = nodeIterator.getPosition() + 1
                                e = node.context[pos]

                                if(e.isPair())
                                    node.context.add(pos, DBElement("A:$a"))
                                else  // e is agg
                                    e.setV("$a")

                                updated = true
                            }
                        }
                    }
                }
            }

            a = node.getA()  // aggregate value of current node
        }
    }

    /**
     * Algorithm for updating a key k in the DB-tree T with a new value v assuming that k is already present in T.
     *
     * @param p the key to be updated and the corresponding new value.
     */
    fun update(p: Pair<K,V>){
        val k = p.first
        val v = p.second

        val N = c.getIncludingK(k).map{ Node(it) }  // retrieving necessary nodes
        N[0].update(k, v)  // modify n (PRE: must contain k)
        updateAggr(N)  // propagate modifications up to the root

        c.update(N.map { it.toTransfer() })
    }

    /**
     * Algorithm for inserting a new key-value pair into the DB-tree T, supposing the key is not yet contained in T.
     *
     * @param p the pair to be inserted.
     */
    fun insert(p: Pair<K, V>){
        val k = p.first
        val v = p.second

        val toCreate = LinkedList<NodeTransfer>()  // nodes that will be created
        val toUpdateContext = LinkedList<NodeTransfer>()  // nodes whose context will be modified
        val toDelete = LinkedList<Triple<Int, String, String>>()  // nodes that will be deleted

        // Retrieve from T all nodes n | n.min < k < n.max, ordered by ascending n.level
        val N = c.getIncludingK(k).map { Node(it) }

        val l = extractLevel("$k")  // The new level

        // Nodes above the just inserted one
        val U = LinkedList<Node>()
        N.filterTo(U, { it.level > l })

        val n_2 = U.minBy { it.level }  // U's node with lowest level

        // Nodes below the just inserted one
        val D = LinkedList<Node>()
        N.filterTo (D, { it.level < l })

        var n_bar : Node  // the node containing the pair to be inserted
        var new_nbar = false  // true if n_bar needs to be created

        // Try to find n_bar. On failure, create it.
        try {  // In case the node exists
            n_bar = N.filter { it.level == l }[0]
            n_bar.insert(p.first, v)
        }
        catch (e: IndexOutOfBoundsException) {  // In case the node doesn't exist

            // node must be created
            new_nbar = true

            // finding k_min/k_max on the upper node
            val (k1, k2) = n_2!!.between(p.first)

            // creating the new node
            val nbar_context = LinkedList<DBElement>()
            nbar_context.add(DBElement("P:($k,$v)"))
            n_bar = Node(l, k1, k2, nbar_context)
        }

        // Initialize two empty sequences of nodes: L, R
        val L = LinkedList<Node>()
        val R = LinkedList<Node>()

        // Step 10
        D.asReversed().forEach {

            // Left partition
            val contextL = it.lowerThan(p.first)
            if(!contextL.isEmpty())
                L.addFirst(Node(it.level, it.k_min, p.first, contextL))

            // Right partition
            val contextR = it.higherThan(p.first)
            if(!contextR.isEmpty())
                R.addFirst(Node(it.level, p.first, it.k_max, contextR))
        }

        // Step 16.1
        L.add(n_bar)
        updateAggr(L)

        // Step 16.2
        R.add(L.pollLast())
        updateAggr(R)

        // Step 17
        U.addFirst(R.pollLast())
        updateAggr(U)

        // Managing nodes' persistence
        D.forEach { toDelete.add(it.toTriple()) }
        L.forEach { toCreate.add(it.toTransfer()) }
        R.forEach { toCreate.add(it.toTransfer()) }
        U.drop(1).forEach { toUpdateContext.add(it.toTransfer()) }

        if(new_nbar)
            toCreate.add(n_bar.toTransfer())
        else
            toUpdateContext.add(n_bar.toTransfer())

        // Executing updates
        c.updateDBTree(toCreate,toUpdateContext,toDelete)
    }

    /**
     * Algorithm for deleting a key k (and the corresponding value) from the DB-tree T, supposing the key is contained
     * in T.
     *
     * @param k the key to delete.
     */
    fun delete(k: K) {

        // Modifications in persistence
        val toCreate = LinkedList<NodeTransfer>()  // nodes that will be created
        val toUpdateContext = LinkedList<NodeTransfer>()  // nodes whose context will be modified
        val toDelete = LinkedList<Triple<Int, String, String>>()  // nodes that will be deleted

        // Retrieving nodes
        val N = c.getDelete(k).map { Node(it) }

        // Identify n_bar
        val n_bar = N.filter { it.contains(k) }[0]

        val l = n_bar.level

        val L = LinkedList<Node?>()
        val R = LinkedList<Node?>()
        val U = LinkedList<Node>()

        N.filterTo(L, { it.level < l && it.k_min < k })
        N.filterTo(R, { it.level < l && it.k_max > k })
        N.filterTo(U, { it.level > l })

        // Persistence
        L.forEach { if(it != null) toDelete.add(it.toTriple()) }
        R.forEach { if(it != null) toDelete.add(it.toTriple()) }

        // Fill empty levels with null(s)
        for(i in 0 until l){

            if (i >= L.size || L[i]!!.level != i)
                L.add(i, null)

            if (i >= R.size || R[i]!!.level != i)
                R.add(i, null)
        }

        // Remove the key from n_bar
        n_bar.removeWithAs(k)

        val D = LinkedList<Node>()
        var n_prev = n_bar  // placeholder for the previous node in the loop
        var toAdd = true  // when true, adds the current node to D

        if(n_bar.isEmpty()) {
            toAdd = false

            // remove the aggregated placeholder on n_bar's upper node
            U[0].removeA(n_bar.k_min, n_bar.k_max)

            // delete n_bar from database
            toDelete.add(n_bar.toTriple())
        }

        // Merging loop
        for(i in l-1 downTo 0) {

            if(toAdd) {
                D.addFirst(n_prev)
                toAdd = false
            }

            // resulting node from the merge
            var n : Node? = null

            // Find neighbours of k inside n_prev
            val (k_min, k_max) = n_prev.between(k)

            // both full
            if(L[i] != null && R[i] != null){

                // context for the new node
                val context = LinkedList<DBElement>()

                // add L[i]'s context
                context.addAll(L[i]!!.context)

                // eventually remove the last aggregated value of L
                if(context[context.size - 1].isAgg())
                    context.removeAt(context.size - 1)

                // R[i]'s context
                val contextR = R[i]!!.context

                // eventually remove the first aggregated value of R
                if(contextR[0].isAgg())
                    contextR.removeAt(0)

                // then add contextR to the new context
                context.addAll(contextR)

                // new merged node
                n = Node(i, k_min, k_max, context)

            }

            // alternatively full
            else if(L[i] != null && R[i] == null){
                n = L[i]!!
                n.k_max = k_max
            }
            else if(L[i] == null && R[i] != null){
                n = R[i]!!
                n.k_min = k_min
            }

            if (n != null) {
                n_prev = n
                toAdd = true
            }
        }

        // eventually add the last iteration's node
        if(toAdd) D.addFirst(n_prev)

        // save D's size up to nbar
        val x = if (n_bar.isEmpty()) 0 else 1

        D.addAll(U)
        updateAggr(D)

        /**
         * Persistence:
         * Note: D = { {merged nodes}, n_bar, U }
         * #{merged nodes} = #D - (#U + 1) -> must be created
         * #{n_bar, U} = 1 + #U -> must be context updated
         */
        D.dropLast(U.size + x).forEach { toCreate.add(it.toTransfer()) }
        D.drop(D.size - U.size - x).forEach { toUpdateContext.add(it.toTransfer()) }

        // Executing updates
        c.updateDBTree(toCreate,toUpdateContext,toDelete)
    }

    /**
     * Algorithm for performing a group-by range query on a DB-tree T.
     *
     * This algorithm can be invoked if and only if the key type is a pair (or a tuple). The key type must be
     * decomposable in the form <X,Y>, where X corresponds to the group type, and Y to the actual keyring system for
     * each group.
     * The algorithm accepts as input the range <y',y">.
     * Neither y' nor y" must be explicitly contained in any T's group.
     * The algorithm computes the aggregate value for the values of all keys between y' and y", for each group in T.
     *
     * @param range the input range.
     * @return the list of aggregate results of the group-by range query, one for each group.
     */
    fun groupBy(range: Pair<*, *>) : List<Pair<Any,D?>> {


        val y1 = range.first!!
        val y2 = range.second!!

        // steps 1,2,3 of the algorithm
        lateinit var L : List<Node>
        lateinit var R : List<Node>
        lateinit var n_bar : Map<Any?, Node>

        val threads : MutableList<Thread> = mutableListOf()
        threads.add( Thread{ L = c.getGroupByL(y1,y2).map{Node(it)} }.apply{start()} )
        threads.add( Thread{ R = c.getGroupByR(y1,y2).map{Node(it)} }.apply{start()} )
        threads.add( Thread{ n_bar = c.getGroupByNBar(y1,y2).mapValues {Node(it.value)} }.apply{start()} )
        threads.forEach { it.join() }

        val listRes = mutableListOf<Deferred<Pair<Any, D?>>>()

        // For every x in the dbtree
        for(x in n_bar.keys){
            val Lx = L.filter { kp.group("${it.k_max}") == x }
            val Rx = R.filter { kp.group("${it.k_min}") == x }
            val k1 = kp.parse("($x/$y1)")
            val k2 = kp.parse("($x/$y2)")
            listRes.add(GlobalScope.async { Pair(x!!, calculateAggr(Lx, Rx, n_bar[x], k1, k2)) })
        }

        return runBlocking { listRes.awaitAll() }
    }

    /**
     * Algorithm for performing an authenticated query on a DB-tree T.
     *
     * This algorithm is the main procedure of an Authenticated DB-tree, but could be employed also when [f] is not
     * an hashing function. In that case this procedures just returns the sequence of steps to perform to obtain the
     * root aggregate value starting from the queried value.
     *
     * @param k the queried key.
     * @return the queried value and the corresponding proof.
     */
    fun authenticatedQuery(k: K): Pair<V?, MutableList<LinkedList<String>>> {

        var N = c.getIncludingK(k).map{Node(it)}  // retrieve necessary nodes
        val n_bar = N.first()  // the node eventually containing key k
        N = N.dropLast(1).drop(1)
        var k_bar: K? = null
        var v_bar : V? = null  // the value associated with k_bar

        val firstNodeContext = LinkedList<String>()
        for(i in n_bar.context) {
            if(i.isPair()){
                if(i.k() == "$k"){  // if it was the k passed like parameter, stop searching
                    k_bar = kp.parse(i.k())
                    v_bar = f.parseV(i.v())
                    firstNodeContext.add(null.toString())
                }else{
                    firstNodeContext.add("${f.g(f.parseV(i.v()))}")
                }
            }else{
                firstNodeContext.add("${f.parseA(i.v())}")
            }
        }

        val proof = mutableListOf<LinkedList<String>>()
        proof.add(firstNodeContext)
        N.forEach{
            val context = LinkedList<String>()
            val n_it = it.iterator()
            var flag = 0

            while(n_it.hasNext()){
                val element = n_it.next()

                if(flag == 0) {
                    if (element.isPair())
                        context.add("${f.g(f.parseV(element.v()))}")

                    else {
                        flag = if (k_bar!! < it.firstKey()) 1
                        else if (n_it.nextPair()?.k() != null && kp.parse(n_it.nextPair()?.k()!!) < k_bar) 0
                        else if (n_it.nextPair()?.k()!=null && kp.parse(n_it.nextPair()?.k()!!) > k_bar) 1
                        else 1

                        context.add(
                            if (flag == 1)
                                null.toString()
                            else
                                "${f.parseA(element.v())}"
                        )
                    }
                }
                else {
                    context.add(
                        if(element.isPair())
                            "${f.g(f.parseV(element.v()))}"
                        else
                            "${f.parseA(element.v())}"
                    )
                }
            }

            proof.add(context)
        }

        return Pair(v_bar, proof)
    }

    /**
     * DB-tree Node class.
     */
    private inner class Node (
        val level: Int,
        var k_min: K,
        var k_max: K,
        val context: LinkedList<DBElement>) : Iterable<DBElement>
    {

        constructor (nt: NodeTransfer) : this(
            nt.level,
            kp.parse(nt.k_min),
            kp.parse(nt.k_max),
            nt.parseContext()
        )

        /**
         * Returns true if the node is at level +inf
         */
        fun isTop() = this.level == Int.MAX_VALUE

        /**
         * Returns true if n.aseq = {}.
         */
        fun isEmpty() = context.isEmpty()

        /**
         * Returns true if the node contains the key k.
         */
        fun contains(k: K): Boolean {

            val e_it = iterator()

            while (e_it.hasNext()){
                val e = e_it.next()

                if(e.isPair() && kp.parse(e.k()) == k)
                    return true
            }

            return false
        }

        /**
         * Given a key, finds the two keys (k_i, k_i+1) in n.aseq s.t.
         * n.min < k_0 < ... < k_i < k < k_i+1 < ... < k_m < n.max.
         * Note: when k_i or k_i+1 don't exist, n.min or n.max are returned instead.
         */
        fun between(k: K) : Pair<K, K> {

            val e_it = iterator()
            var k1 = this.k_min
            var k2 = this.k_max

            // keep scrolling until it's found an higher key
            var found = false

            while(! found && e_it.hasNext()){

                val e = e_it.next()

                if(e.isPair()){

                    val e_k = kp.parse(e.k())

                    if (e_k < k)
                        k1 = e_k

                    if (k < e_k) {
                        k2 = e_k
                        found = true
                    }

                }

            }

            return Pair(k1, k2)
        }

        /**
         * Calculates the aggregated value of the current node.
         *
         * Let n be this node | n.aseq = {a_0, p_1, ..., p_m, a_m}.
         *
         * @return f(n) | f(a_0, g(p_1), ..., g(p_m), a_m).
         */
        fun getA() : A?{

            val aggList = ArrayList<A?>()  // the list of all aggregated values for the current node
            val nodeIterator = this.iterator()  // node iterator

            while(nodeIterator.hasNext()){
                val e = nodeIterator.next()  // consider next element
                aggList.add(
                    if (e.isAgg()) f.parseA(e.v())
                    else f.g(f.parseV(e.v()))
                )
            }

            return f.f(aggList)
        }

        /**
         * Returns the first key of the context.
         */
        fun firstKey() = kp.parse(firstPair().k())

        /**
         * Returns the last key of the context.
         */
        fun lastKey() = kp.parse(lastPair().k())

        /**
         * Returns the first pair of the context.
         */
        fun firstPair() : DBElement {

            // access the first element
            var e = context[0]

            if (e.isAgg())
                e = context[1]

            return e
        }

        /**
         * Returns the last pair of the context.
         */
        fun lastPair() : DBElement {

            // access the last element
            var e = context[context.size - 1]

            if (e.isAgg())
                e = context[context.size-2]

            return e
        }

        /**
         * Creates (or replaces) the first aggregated value of the context.
         */
        fun setFirstA(a: A?) {

            if (context.size > 0 && context[0].isAgg())
                context[0] = DBElement("A:$a")
            else
                context.addFirst(DBElement("A:$a"))
        }

        /**
         * Creates (or replaces) the last aggregated value of the context (a_m).
         */
        fun setLastA(s: A?) {

            // last accessible index
            val x = context.size - 1

            if (context.size > 0 && context[x].isAgg())
                context[x] = DBElement("A:$s")

            else // create it
                context.add(DBElement("A:$s"))
        }

        /**
         * Updates the value of k in the current node's context.
         *
         * PRE: k must be contained inside the node.
         *
         * @param k the key affected by the update.
         * @param v the new value.
         */
        fun update(k: K, v: V) {
            // for each element in the context
            context.forEach {
                if (it.isPair() && it.k() == "$k"){
                    it.setV("$v")
                }
            }
        }

        /**
         * Inserts a new key-value pair into the current node.
         *
         * PRE: (k,v) must not be contained into the node.
         * POST: (k,v) is inserted into the node, according to the invariants of the DB-tree.
         *
         * @param k the key to be inserted.
         * @param v the corresponding value.
         */
        fun insert(k: K, v: V){

            // the element to be inserted
            val p = DBElement("P:($k,$v)")

            var inserted = false  // not inserted yet

            // iterate through the elements of the node
            val n_it = iterator()

            while (!inserted && n_it.hasNext()){

                val e = n_it.next()

                // position where to insert the new k,v pair in the node context
                val pos = n_it.getPosition()

                // if 'e' is a pair with higher key, insert p before
                if(e.isPair() && k < kp.parse(e.k())) {

                    // insert pair at position pos
                    context.add(pos, p)

                    // remove the previous aggregated value
                    if (pos > 0 && context[pos-1].isAgg())
                        context.removeAt(pos-1)
                    inserted = true
                }
            }

            if(!inserted){
                if(context[context.size - 1].isAgg())
                    context.removeAt(context.size - 1)
                context.addLast(p)
            }
        }

        /**
         * Removes the pair associated with the provided key from the context
         * and the two aggregated values nearby (when present).
         *
         * I.e.: {a_0, p_1, ... , a_k-1, p_k, a_k, ... , p_m, a_m} -> {a_0, p_1, ... , p_k-1, p_k+1, ... , p_m, a_m}
         *
         * @param k the key to be removed.
         */
        fun removeWithAs(k: K){

            val e_it = iterator()
            var deleted = false

            while (!deleted && e_it.hasNext()){
                val e = e_it.next()

                if(e.isPair() && kp.parse(e.k()) == k) {

                    // take the iterator's position
                    var pos = e_it.getPosition()

                    // remove the element
                    context.removeAt(pos)

                    if(pos < context.size && context[pos].isAgg())
                        context.removeAt(pos)

                    if(--pos >= 0 && pos < context.size && context[pos].isAgg())
                        context.removeAt(pos)

                    deleted = true
                }
            }
        }

        /**
         * Remove the aggregated value between two keys.
         */
        fun removeA(k1: K, k2: K){

            when {
                k1 == k_min -> context.removeAt(0)
                k2 == k_max -> context.removeAt(context.size - 1)
                else -> {
                    val n_it = iterator()

                    while(n_it.hasNext()){
                        val e = n_it.next()

                        if (e.isAgg()){

                            val p_prev = n_it.prevPair()
                            val p_next = n_it.nextPair()

                            if (p_prev != null && p_next != null &&
                                kp.parse(p_prev.k()) == k1 && kp.parse(p_next.k()) == k2)
                                context.remove(e)
                        }

                    }

                }
            }


        }

        /**
         * Given a key k, returns the partition of n.aseq = {a_0, p_1, ..., p_j}, where
         * p_j+1 = (k_j+1, v_j+1) is excluded.
         *
         * Note: a_j is not returned, neither.
         *
         * @param k the k_j, upper limit (included) of the returned partition.
         */
        fun lowerThan(k: K) : LinkedList<DBElement> {

            val res = LinkedList<DBElement>()

            val n = iterator()

            while (n.hasNext()) {

                // the next element in n.aseq
                val e = n.next()

                if (e.isPair()){
                    if (kp.parse(e.k()) < k)
                        res.add(e)
                }

                else {

                    // if it was the last aggregated value, stop searching
                    if(!n.hasNext())
                        break

                    val next = n.next()

                    // if the next key is associated with an higher key, stop searching
                    if(kp.parse(next.k()) > k)
                        break

                    res.add(e)
                    res.add(next)
                }
            }

            return res
        }

        /**
         * Given a key k_j, returns the partition of n.aseq = {p_j+1, ..., p_m}, where
         * p_j = (k_j, v_j) is excluded.
         *
         * Note: a_j+1 is not considered.
         *
         * @param k the k_j, lower limit (excluded) of the returned partition.
         */
        fun higherThan(k: K) : LinkedList<DBElement> {

            val res = LinkedList<DBElement>()

            val n_it = iterator()

            while (n_it.hasNext()) {

                // the next element in n.aseq
                var e = n_it.next()

                if (e.isPair() && kp.parse(e.k()) > k) {

                    // take it
                    res.add(e)

                    // take all next elements
                    while(n_it.hasNext()){
                        e = n_it.next()
                        res.add(e)
                    }

                }

            }

            return res
        }

        /**
         * Returns a triple containing the primary key of the node.
         */
        fun toTriple() = Triple(level, "$k_min", "$k_max")

        /**
         * Converts the current node to a Node Transfer.
         */
        fun toTransfer() : NodeTransfer {

            val contextSB = StringBuilder()
            this.context.forEach {
                contextSB.append(it.toString())
                contextSB.append(';')
            }

            return NodeTransfer(
                level,
                "$k_min",
                "$k_max",
                "$contextSB"
            )

        }

        /**
         * Returns a string representation of the current node.
         */
        override fun toString() = "Level: $level; K_min: $k_min; K_max: $k_max; Context: $context"

        /**
         * Returns an iterator over the context.
         */
        override fun iterator() : NodeIterator {
            return NodeIterator()
        }

        /**
         * Node Iterator class.
         */
        inner class NodeIterator : Iterator<DBElement> {

            /**
             * The next position to be returned.
             */
            private var position: Int = 0

            /**
             * Returns true if the context has another element wrt the current position.
             */
            override fun hasNext(): Boolean {
                return position < context.size
            }

            /**
             * Returns the next element in the context.
             */
            override fun next(): DBElement {
                return context[position++]
            }

            /**
             * Returns the actual position of the iterator over the context.
             */
            fun getPosition() = position - 1

            /**
             * Returns the first pair after the current position, null otherwise.
             */
            fun nextPair() : DBElement? {

                if (!hasNext())
                    return null

                val e = context[position]

                if (e.isPair())
                    return e

                else if (position + 1 < context.size){
                    return context[position+1]
                }

                // should never go here, if PRE are respected
                return null
            }

            /**
             * Returns the first pair before the current position, null otherwise.
             *
             * PRE: iterator must be positioned on a A value.
             */
            fun prevPair() : DBElement? {

                if (position <= 1)  // position == 0 || position == 1
                    return null

                return context[position - 2]
            }

        }

    }


    // ============== INSERT BATCH =================== //

    /**
     * Algorithm for inserting a batch of new key-value pairs into an empty DB-tree.
     *
     * The whole process is performed in-memory, using the hierarchical data structure BatchNode, and only committed to
     * the DBMS when all pairs have been processed.
     *
     * @param list the list of pairs to be inserted.
     */
    fun insertBatch(list: List<Pair<K, V>>) {

        // Initializing the top dbtree node
        val dbtree = BatchNode(
            maxLevel,
            kp.getMin(),
            kp.getMax(),
            LinkedList(),
            LinkedList())

        list.forEach { pair ->
            val k = pair.first
            val v = pair.second

            val l = extractLevel("$k")  // The new level

            // Insert the new pair in the dbtree
            auxBatchInsert(k, v, l, dbtree)
        }

        // Convert the batch nodes into node transfers, for the dbms population
        // Here is also where aggregated values are calculated
        val nodes = LinkedList<NodeTransfer>()
        convert(dbtree, nodes)

        // Delete the preexisting top-level node
        c.delete(Triple(maxLevel, "${kp.getMin()}", "${kp.getMax()}"))

        // Export the nodes on the DBMS
        c.create(nodes)
    }

    /**
     * Converts a BatchNode into a NodeTransfer list, calculating the corresponding aggregate values.
     *
     * @param n the BatchNode to be converted.
     * @param list the NodeTransfer list pointer to be populated.
     * @return the aggregate value of n.
     */
    private fun convert(n: BatchNode, list: LinkedList<NodeTransfer>) : A? {

        // NodeTransfer for the current BatchNode (n)
        val nodeTransfer = NodeTransfer(
            n.level,
            n.k_min.toString(),
            n.k_max.toString(),
            ""
        )

        val aggrList = ArrayList<A?>()  // buffer for the aggregated values for the current nodes
        var i = 0  // iterator over descendants
        var j = 0  // iterator over pairs

        // Until all elements are checked (descendants + pairs)
        while (i < n.descendants.size || j < n.context.size){

            // If there're both descendants and pairs, choose the smaller key one
            if(i < n.descendants.size && j < n.context.size){

                // When the descendant comes first...
                if(n.descendants[i].k_min < n.context[j].first){

                    // Convert the descendant, saving its aggregated value
                    val i_agg = convert(n.descendants[i], list)

                    // Add the aggregated value to the NodeTransfer
                    nodeTransfer.context = nodeTransfer.context.plus("A:$i_agg;")

                    aggrList.add(i_agg)
                    i++
                }

                // When the pair comes first...
                else{

                    // Extract the pair's value
                    val i_agg = f.g(n.context[j].second)

                    // Add the pair to the NodeTransfer
                    nodeTransfer.context = nodeTransfer.context.plus("P:(${n.context[j].first},${n.context[j].second});")

                    aggrList.add(i_agg)
                    j++
                }

            }

            // If there're only descendants left
            else if(i < n.descendants.size){

                // Convert the descendant, saving its aggregated value
                val i_agg = convert(n.descendants[i], list)

                // Add the aggregated value to the NodeTransfer
                nodeTransfer.context = nodeTransfer.context.plus("A:$i_agg;")

                aggrList.add(i_agg)
                i++
            }

            // If there're only pairs left
            else{

                // Extract the pair's value
                val i_agg = f.g(n.context[j].second)

                // Add the pair to the NodeTransfer
                nodeTransfer.context = nodeTransfer.context.plus("P:(${n.context[j].first},${n.context[j].second});")

                aggrList.add(i_agg)
                j++
            }

        }

        // Add the current node to the list
        list.add(nodeTransfer)

        return f.f(aggrList)
    }

    /**
     * Insertion of a new pair <k,v> into the current BatchNode.
     *
     * The new pair is inserted if and only if range and level match the invariants.
     * When there's no matching, the algorithm proceeds over the descendants.
     */
    private fun auxBatchInsert(k: K, v: V, l:Int, n: BatchNode) {

        // If levels do match
        if(n.level == l){

            // Insert the pair into the node
            n.insert(Pair(k,v))

            // Correct descendants
            splitDesc(k, n)
        }

        else{

            // Continue over descendants
            descBatchInsert(k, v, l, n)
        }
    }

    /**
     * Insertion over BatchNode descendants.
     *
     * There are 4 possible scenarios:
     * 1) The pair has to be inserted into one descendant.
     * 2) A new node has to be created, ancestor of the descendants.
     * 3) A new node is created, sibling of the descendants.
     * 4) The algorithm continues, over one descendant.
     */
    private fun descBatchInsert(k: K, v: V, l:Int, n: BatchNode) {

        var k_prev = n.k_min

        // Check all descendants
        for(i in 0 until n.descendants.size){

            // The current descendant
            val curr = n.descendants[i]

            // Interstitial insertion (outside descendants)
            if(k_prev < k && k < curr.k_min ){

                val p = n.findNearest(k)

                // Create the new node
                val newBatchNode = BatchNode(l, p.first , p.second, LinkedList(), LinkedList())
                // Insert the new pair
                newBatchNode.insert(Pair(k,v))
                // Add as n's i-th descendant
                n.descendants.add(i, newBatchNode)

                return
            }

            // In-Range insertion (inside a descendant)
            else if(curr.k_min < k && k < curr.k_max ){

                // Ancestor insertion
                if (l > curr.level){
                    // Create the new node
                    val newBatchNode = BatchNode(l, curr.k_min, curr.k_max, LinkedList(), LinkedList())
                    // Insert the new pair
                    newBatchNode.insert(Pair(k,v))
                    // Establish hierarchies
                    newBatchNode.descendants.add(curr)
                    n.descendants[i] = newBatchNode

                    // Correct descendants
                    splitDesc(k, newBatchNode)

                    return
                }

                // The algorithm continues on the current descendant
                else{
                    auxBatchInsert(k, v, l, curr)
                    return
                }

            }

            k_prev = curr.k_max
        }

        // Last interstitial check
        if (k_prev < k && k < n.k_max ){

            val p = n.findNearest(k)

            // Create the new node
            val newBatchNode = BatchNode(l, p.first, p.second, LinkedList(), LinkedList())
            // Insert the new pair
            newBatchNode.insert(Pair(k,v))
            // Add it as last n's descendant
            n.descendants.addLast(newBatchNode)
        }
    }

    /**
     * Corrects the descendants after the insertion of a new key.
     */
    private fun splitDesc(k : K, n : BatchNode) {

        for (i in 0 until n.descendants.size){

            val curr = n.descendants[i]

            // Found the descendant to correct
            if(curr.k_min < k && k < curr.k_max ){

                // Correct it
                val p = auxSplitDesc(k, curr)

                // Old descendant is replaced by its split portions
                n.descendants.removeAt(i)

                if(p.second != null){
                    n.descendants.add(i, p.second!!)
                }
                if(p.first != null){
                    n.descendants.add(i, p.first!!)
                }

            }

        }

    }

    /**
     * Splits n and its descendants, correcting hierarchies.
     */
    private fun auxSplitDesc(k : K, n : BatchNode) : Pair<BatchNode?, BatchNode?> {

        // Split n
        val p = n.split(k)

        // Preparing the result
        var left = p.first
        var right = p.second

        var i = 0

        while (i < n.descendants.size){

            val curr = n.descendants[i]

            // Found the descendant to correct
            if(curr.k_min < k && k < curr.k_max){

                // Correct and split the descendant
                val d = auxSplitDesc(k, curr)

                // Correct hierarchies
                if(d.first != null){

                    if(left != null)
                        left.descendants.addLast(d.first)

                    else
                        left = d.first
                }
                if(d.second != null){

                    if(right != null)
                        right.descendants.addFirst(d.second)

                    else
                        right = d.second
                }

            }

            // Correct descendant, to assign to the left partition
            else if(curr.k_max < k){
                p.first!!.descendants.addLast(curr)
            }

            // Correct descendant, to assign to the right partition
            else {
                p.second!!.descendants.addLast(curr)
            }

            i++
        }

        return Pair(left, right)
    }

    /**
     * Auxiliary node type, dedicated to the insertBatch algorithm.
     */
    inner class BatchNode(
        val level: Int,
        var k_min: K,
        var k_max: K,
        val context: LinkedList<Pair<K,V>>,
        val descendants: LinkedList<BatchNode>)
    {

        /**
         * Inserts a new pair into the batch node context, respecting key-ordering.
         */
        fun insert(p : Pair<K,V>) {

            var inserted = false  // not inserted yet
            var i = 0  // iterator

            while(!inserted && i < context.size){

                if (p.first < context[i].first){
                    context.add(i, p)
                    inserted = true
                }

                i++
            }

            if(!inserted)
                context.add(i, p)
        }

        /**
         * Splits the current node according to a certain key.
         */
        fun split(k : K) : Pair<BatchNode?, BatchNode?> {
            val leftContext = LinkedList<Pair<K,V>>()
            val rightContext = LinkedList<Pair<K,V>>()

            var left : BatchNode? = null
            var right : BatchNode? = null

            var i = 0  // context iterator

            while(i < context.size){

                if (context[i].first < k){
                    leftContext.addLast(context[i])
                }
                else{
                    rightContext.addLast(context[i])
                }

                i++
            }

            if(!leftContext.isEmpty()){
                left = BatchNode(level, k_min, k , leftContext, LinkedList())
            }

            if(!rightContext.isEmpty()){
                right = BatchNode(level, k , k_max, rightContext, LinkedList())
            }

            return Pair(left, right)
        }

        /**
         * Returns the batchNode keys (eventually k_min/k_max) nearest to the given key.
         */
        fun findNearest(k : K) : Pair<K, K> {
            var k_prev = k_min

            for (i in 0 until context.size){
                val k_next = context[i].first

                if(k_prev < k && k < k_next)
                    return Pair(k_prev , k_next)

                k_prev = k_next
            }

            return Pair(k_prev , k_max )
        }
    }

}
