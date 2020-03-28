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

import java.io.Serializable


/**
 * Abstract class that extends the KeyParser interface.
 *
 * The KeyParserGroupBy is an auxiliary abstract class that adds more information about the key that are needed
 * to perform the groupBy operation.
 *
 * All keys used in a groupBy query can be summarize as a pair (x,y) where x is the most significant part and identify
 * a group and y is the least significant part that identify the range.
 * The KeyParserGroupBy helps the Connector to identify the x and y part of a key.
 *
 * @param K the comparable Key.
 */
abstract class KeyParserGroupBy<K> : KeyParser<K> where K: Comparable<K>, K: Serializable{

    /**
     * @param mostSigignificantDefinition is the list of pair (columnName, columnDefinition) that on the database
     * identify the most significant part of K.
     */
    abstract val mostSignificantDefinition : List<Pair<String,String>>

    /**
     * @param leastSignificantDefinition is the list of pair (columnName, columnDefinition) that on the database
     * identify the least significant part of K.
     */
    abstract val leastSignificantDefinition : List<Pair<String,String>>


    override val keyColumnsDefinitionDB by lazy { mostSignificantDefinition.plus(leastSignificantDefinition)}

    /**
     * Returns a key K.
     *
     * @param x the list of all element that represent the most significant part of the key.
     * @param y the list of all element that represent the least significant part of the key.
     * @return the corresponding key.
     */

    abstract fun bind(x: List<Any>, y: List<Any>): K

    /**
     * Returns the group of the passed composite key.
     *
     * @param s the string representation of a composite key.
     * @return the corresponding list of group components.
     */

    abstract fun group(s: String): List<Any>


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
     * @param y the list of the all elements that represents the least significant part of a key K.
     * @return the string to be interpreted by underlying database.
     */
    abstract fun lsp_ltDB(columnName: List<String>, y: List<Any>): String

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
     * @param y the list of the all elements that represents the least significant part of a key K.
     * @return the string to be interpreted by underlying database.
     */
    abstract fun lsp_leqDB(columnName: List<String>, y: List<Any>): String

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
     * @param y the list of the all elements that represents the least significant part of a key K.
     * @return the string to be interpreted by underlying database.
     */
    abstract fun lsp_gtDB(columnName: List<String>, y: List<Any>): String

    /**
     * DB query for 'greater than or equal' relationship.
     *
     * Returns the string querying underlying DB for all elements having key (attribute) greater than or equal to
     * a given value. Underlying dataset must know how to interpret the query, and return the expected result.
     ** @param columnName the list of the names of the database columns interested in the comparison
     * (they represent the k_min or k_max columns); since this method is only
     * invoked from connector's queries, this field must identify K in the database
     * (it is recommended to use this.keyColumnsDefinitionDB() to generate this list).
     * @param y the list of the all elements that represents the least significant part of a key K.
     * @return the string to be interpreted by underlying database.
     */
    abstract fun lsp_geqDB(columnName: List<String>, y: List<Any>): String

    /**
     * Let k_min = (x_min, y_min).
     * Let k_max = (x_max, y_max).
     *
     * DB query for 'x_min not equal to x_max' relationship.
     *
     * Returns the string querying underlying DB for all elements having  k_min and k_max with a different most significant part.
     * Underlying dataset must know how to interpret the query, and return the expected result.
     *
     * @param columnNameMin the list of the names of the database columns interested in the comparison
     * (they represent the x_min).
     * @param columnNameMax the list of the names of the database columns interested in the comparison
     * (they represent the x_max).
     * @return the string to be interpreted by underlying database as x_min != x_max.
     */
    abstract fun most_neqDB(columnNameMin: List<String>, columnNameMax: List<String>): String

}