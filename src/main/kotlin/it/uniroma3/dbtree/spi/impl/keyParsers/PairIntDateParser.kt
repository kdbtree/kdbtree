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
import java.io.Serializable
import java.sql.ResultSet

/**
 * Example of [KeyParser] class, managing key: [PairIntDateKey].
 *
 */
class PairIntDateParser:
    KeyParser<PairIntDateKey>
{

    override fun getMin() = PairIntDateKey.getMin()

    override fun getMax() = PairIntDateKey.getMax()

    override fun parse(s: String): PairIntDateKey {
        val p = s.substring(1, s.length - 1).split('/')
        return PairIntDateKey(p[0].toInt(), p[1])
    }


    // ---------------- DB TRANSLATIONS ------------------ //

    override val keyColumnsDefinitionDB= listOf(
            Pair("int", "INTEGER NOT NULL"),
            Pair("date", "timestamp NOT NULL")
        )

    override fun ltDB(columnName: List<String>, k: PairIntDateKey): String =
        "(${columnName[0]} < ${k.first} OR (${columnName[0]} = ${k.first} AND ${columnName[1]} < '${k.second}'))"

    override fun leqDB(columnName: List<String>, k: PairIntDateKey): String =
        "(${columnName[0]} < ${k.first} OR (${columnName[0]} = ${k.first} AND ${columnName[1]} <= '${k.second}'))"

    override fun gtDB(columnName: List<String>, k: PairIntDateKey): String =
        "(${columnName[0]} > ${k.first} OR (${columnName[0]} = ${k.first} AND ${columnName[1]} > '${k.second}'))"

    override fun geqDB(columnName: List<String>, k: PairIntDateKey): String =
        "(${columnName[0]} > ${k.first} OR (${columnName[0]} = ${k.first} AND ${columnName[1]} >= '${k.second}'))"

    override fun eqDB(columnName: List<String>, k: PairIntDateKey): String =
        "(${columnName[0]} = ${k.first} AND ${columnName[1]} = '${k.second}')"

    override fun toDB(k: PairIntDateKey) = "(${k.first}, '${k.second}')"

    override fun toNodeTransfer(rs: ResultSet): NodeTransfer {
        return NodeTransfer(
            rs.getInt(1),
            "(${rs.getString(2)}/${rs.getString(3)})",
            "(${rs.getString(4)}/${rs.getString(5)})",
            rs.getString(6)
        )
    }
}

/**
 * The Key type maintained by the PairIntDateParser.
 *
 * @property first the group.
 * @property second the least significant component of the key.
 */
class PairIntDateKey(
    var first: Int,
    var second: String
): Comparable<PairIntDateKey>, Serializable
{
    override fun compareTo(other: PairIntDateKey) = comparator.compare(this, other)

    override fun toString() = "($first/$second)"

    override fun equals(other: Any?): Boolean {
        if (other == null)
            return false
        if (other.javaClass != this.javaClass)
            return false
        return compareTo(other as PairIntDateKey) == 0
    }

    companion object {
        val comparator = compareBy(PairIntDateKey::first, PairIntDateKey::second)
        fun getMin() =
            PairIntDateKey(Int.MIN_VALUE, "-infinity")
        fun getMax() =
            PairIntDateKey(Int.MAX_VALUE, "infinity")
    }
}