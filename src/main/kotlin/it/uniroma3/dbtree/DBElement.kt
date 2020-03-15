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

/**
 * Class representing the atomic component of each node.
 *
 * Can be either an aggregated value (A:x), or a key-value pair (P:(x,y)).
 * The discrimination happens by means of the boolean
 * @field isAgg: that goes true if the content is an aggregated value.
 *
 * The DBElement stores the values of pairs and aggregated values
 * in the form of Strings (i.e. they are always stored as non-parsed,
 * hence not in their actual types).
 * Parsing to the actual type is performed every time we need to execute
 * some operation (e.g. comparisons, etc) with the actual content of the
 * DBElement.
 */
class DBElement(

        /**
         * Schema illustrated through an example: Arithmetic mean
         * "A:(10,28)"
         *      aggregated value where 10 -> #elems; 28 -> sum of their values
         * "P:(10,28)"
         *      pair indicating a single element, where 10 -> key; 28 -> value
         */
        private var content: String
)
{
    /**
     * The actual content of the current DBElement.
     *
     * If the DBElement is a pair P:(x,y), the corresponding bareContent is: x,y
     * If the DBElement is an aggregate value A:x, the corresponding bareContent is: x
     */
    private var bareContent : String =
            if (isAgg()) content.substring(2, content.length)
            else content.substring(3, content.length - 1)

    /**
     * Returns the key of the element.
     * PRE: value is not A.
     */
    fun k() : String {
        return bareContent.split(',')[0]
    }

    /**
     * @return the string representation of the value.
     */
    fun v() : String {
        if (isAgg()) return bareContent
        return bareContent.split(',')[1]
    }

    /**
     * Sets the value of a DBElement.
     */
    fun setV(s: String) {
        if(isAgg()){
            bareContent = s
            content = "A:$bareContent"
        }
        else{
            bareContent = "${k()},$s"
            content = "P:($bareContent)"
        }

    }

    /**
     * @return true if the DBElement is an aggregated value.
     */
    fun isAgg() = content[0] == 'A'

    /**
     * @return true if the DBElement is a key-value pair.
     */
    fun isPair() = content[0] == 'P'

    override fun toString() = content
}