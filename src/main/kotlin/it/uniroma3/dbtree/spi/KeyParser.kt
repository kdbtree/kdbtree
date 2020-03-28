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
import java.sql.ResultSet

/**
 * Interface for the KeyParser.
 *
 * The KeyParser is an auxiliary interface managing all information related with the key.
 * In particular, the KeyParser provides the following services: (I) key parsing, (II) key comparison
 * translation for database queries, (III) key definition for database schema.
 * In particular, to fulfill services (II) and (III), KeyParser must be aware of the underlying database technology.
 *
 * @param K the comparable Key.
 */
interface KeyParser <K> where K: Comparable<K>, K: Serializable
{


    /**
     * Returns the maximum value for the managed key type.
     *
     * @return the K's maximum value.
     */
    fun getMax(): K

    /**
     * Returns the minimum value for the managed key type..
     *
     * @return the K's minimum value.
     */
    fun getMin(): K

    /**
     * Parses a key given the corresponding serialized representation.
     *
     * @param s the key passed as string. This string is obtained from K.toString()
     * @return the corresponding K representation.
     */
    fun parse(s: String): K


    /* ... ................ DB TRANSLATIONS ................
     *
     * This section contains all those methods enhancing
     * the usage of composite keys, whose interface
     * may be missing by the underlying database.
     *
     * The client must decompose the structure of
     * composite keys (which can be interpreted as such by
     * the DB-tree in main memory) into simpler attributes
     * having primitive types, whose definitions and comparisons
     * must be understood and interpreted by the underlying database.
     *
     * Such operations are necessary unless the underlying database
     * provides some way to directly define custom types.
     */


    /**
     * It is the list of all the column names and their definition that identify a key.
     *
     * This method provides the column scheme for the key type (K) managed by the current KeyParser object.
     * This method will be used by the Connector class at the moment of defining the table persisting the
     * DB-tree.
     *
     * @param keyColumnsDefinitionDB the list of pairs: <columnName, columnDefinition>. ColumnDefinition must
     * be understood and interpreted by the underlying database to define the the corresponding column.
     *
     */
    val keyColumnsDefinitionDB: List<Pair<String,String>>

    
    /**
     * DB query for 'less than' relationship.
     *
     * Returns the string querying underlying DB for all elements having key (attribute) less than a given value.
     * Underlying dataset must know how to interpret the query, and return the expected result.
     *
     * @param columnName the list of the names of the database columns interested in the comparison
     * (they represent the k_min or k_max columns); since this method is only
     * invoked from connector's queries, this field must identify K in the database
     * (it is recommended to use this.keyColumnsDefinitionDB() to generate this list).
     * @param k the key soil value.
     * @return the string to be interpreted by underlying database as 'k_min < k' ('k_max < k').
     */
    fun ltDB(columnName: List<String>, k: K): String

    /**
     * DB query for 'less than or equal' relationship.
     *
     * Returns the string querying underlying DB for all elements having key (attribute) less than or equal to
     * a given value. Underlying dataset must know how to interpret the query, and return the expected result.
     *
     * @param columnName the list of the names of the database columns interested in the comparison
     * (they represent the k_min or k_max columns); since this method is only
     * invoked from connector's queries, this field must identify K in the database
     * (it is recommended to use this.keyColumnsDefinitionDB() to generate this list).
     * @param k the key soil value.
     * @return the string to be interpreted by underlying database as 'k_min <= k' ('k_max <= k').
     */
    fun leqDB(columnName: List<String>, k: K): String

    /**
     * DB query for 'greater than' relationship.
     *
     * Returns the string querying underlying DB for all elements having key (attribute) greater than a given value.
     * Underlying dataset must know how to interpret the query, and return the expected result.
     *
     * @param columnName the list of the names of the database columns interested in the comparison
     * (they represent the k_min or k_max columns); since this method is only
     * invoked from connector's queries, this field must identify K in the database
     * (it is recommended to use this.keyColumnsDefinitionDB() to generate this list).
     * @param k the key soil value.
     * @return the string to be interpreted by underlying database as 'k_min > k' ('k_max > k').
     */
    fun gtDB(columnName: List<String>, k: K): String

    /**
     * DB query for 'greater than or equal' relationship.
     *
     * Returns the string querying underlying DB for all elements having key (attribute) greater than or equal to
     * a given value. Underlying dataset must know how to interpret the query, and return the expected result.
     ** @param columnName the list of the names of the database columns interested in the comparison
     * (they represent the k_min or k_max columns); since this method is only
     * invoked from connector's queries, this field must identify K in the database
     * (it is recommended to use this.keyColumnsDefinitionDB() to generate this list).
     * @param k the key soil value.
     * @return the string to be interpreted by underlying database as 'k_min >= k' ('k_max >= k').
     */
    fun geqDB(columnName: List<String>, k: K): String

    /**
     * DB query for 'equal to' relationship.
     *
     * Returns the string querying underlying DB for all elements having key (attribute) equal to a given value.
     * Underlying dataset must know how to interpret the query, and return the expected result.
     *
     * @param columnName the list of the names of the database columns interested in the comparison
     * (they represent the k_min or k_max columns); since this method is only
     * invoked from connector's queries, this field must identify K in the database
     * (it is recommended to use this.keyColumnsDefinitionDB() to generate this list).
     * @param k the key soil value.
     * @return the string to be interpreted by underlying database as 'k_min = k' ('k_max = k').
     */
    fun eqDB(columnName: List<String>, k: K): String

    /**
     * Returns the full representation of a key into the underlying database.
     *
     * The DB's representation, in fact, may involve multiple columns for a single key type.
     *
     * @param k the key to be represented.
     * @return its DB representation, like: "attr1, ..., attrN".
     */
    fun toDB(k: K): String



    /**
     * Converts the ResultSet of a DB tuple into the corresponding NodeTransfer.
     *
     * This realizes the conversion from the database representation to the in-memory one.
     *
     * @param rs the ResultSet returned from a query.
     * @return the corresponding NodeTransfer representation.
     */
    fun toNodeTransfer(rs: ResultSet): NodeTransfer


}


