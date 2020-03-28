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

package it.uniroma3.dbtree.spi.impl.keyParsers


import it.uniroma3.dbtree.spi.KeyParser
import it.uniroma3.dbtree.NodeTransfer
import java.sql.ResultSet

/**
 * Example of [KeyParser] class, managing simple key: Int.
 *
 */
class IntParser:
    KeyParser<Int>
{

    override fun getMin() = Int.MIN_VALUE

    override fun getMax() = Int.MAX_VALUE

    override fun parse(s: String) = s.toInt()



    // ---------------- DB TRANSLATIONS ------------------ //

    override val keyColumnsDefinitionDB = listOf(Pair("key", "INTEGER NOT NULL"))



    override fun ltDB(columnName: List<String>, k: Int) =
        "${columnName.first()} < $k"

    override fun leqDB(columnName: List<String>, k: Int) =
        "${columnName.first()} <= $k"

    override fun gtDB(columnName: List<String>, k: Int) =
        "${columnName.first()} > $k"

    override fun geqDB(columnName: List<String>, k: Int) =
        "${columnName.first()} >= $k"

    override fun eqDB(columnName: List<String>, k: Int) =
        "${columnName.first()} = $k"

    override fun toDB(k: Int) = "$k"


    override fun toNodeTransfer(rs: ResultSet): NodeTransfer {
        return NodeTransfer(
            rs.getInt(1),
            rs.getString(2),
            rs.getString(3),
            rs.getString(4)
        )
    }



}