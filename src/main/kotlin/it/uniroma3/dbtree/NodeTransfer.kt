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

import java.util.*

/**
 * DTO for a DB-tree node, employed for mediating communications with the database.
 *
 * This DTO is necessary due to the possible conversion in the representation of the keys(and elements nodes) from
 * database world to the in-memory one. NodeTransfer stores keys and context in the form of strings.
 */
class NodeTransfer(
    var level: Int,
    var k_min: String,
    var k_max: String,
    var context: String
) {

    /**
     * Parse a DTO context into a DBTree one
     *
     * @return
     */
    fun parseContext() : LinkedList<DBElement> {
        val res = LinkedList<DBElement>()
        context.split(';').forEach { if(it != "") res.add(DBElement(it)) }
        return res
    }

    override fun toString() = "Level: $level; K_min: $k_min; K_max: $k_max; Context: $context"
}