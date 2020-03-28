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

import it.uniroma3.dbtree.NodeTransfer
import it.uniroma3.dbtree.spi.KeyParser
import java.io.Serializable
import java.sql.ResultSet

/**
 * Example of [KeyParser] class, managing a groupBy key: [Person].
 * A Person is identify by firstName and lastName
 */

class PersonParser : KeyParser<Person>{

    override fun getMax() = Person.getMax()

    override fun getMin() = Person.getMin()

    override fun parse(s: String) : Person {
        val tmp = s.split(" ")
        return Person(tmp[0],tmp[1])
    }

    override val keyColumnsDefinitionDB = listOf(
        Pair("firstname", "CHAR(30) NOT NULL"),
        Pair("lastname", "CHAR(30) NOT NULL")
    )

    override fun ltDB(columnName: List<String>, k: Person) =
        "(${columnName[0]} < '${k.firstName}' OR (${columnName[0]} = '${k.firstName}' AND ${columnName[1]} < '${k.lastName}'))"


    override fun leqDB(columnName: List<String>, k: Person) =
        "(${columnName[0]} < '${k.firstName}' OR (${columnName[0]} = '${k.firstName}' AND ${columnName[1]} <= '${k.lastName}'))"


    override fun gtDB(columnName: List<String>, k: Person) =
        "(${columnName[0]} > '${k.firstName}' OR (${columnName[0]} = '${k.firstName}' AND ${columnName[1]} > '${k.lastName}'))"


    override fun geqDB(columnName: List<String>, k: Person) =
        "(${columnName[0]} > '${k.firstName}' OR (${columnName[0]} = '${k.firstName}' AND ${columnName[1]} >= '${k.lastName}'))"


    override fun eqDB(columnName: List<String>, k: Person) =
        "(${columnName[0]} = '${k.firstName}' AND ${columnName[1]} = '${k.lastName}')"


    override fun toDB(k: Person) =
        "'${k.firstName}', '${k.lastName}'"

    override fun toNodeTransfer(rs: ResultSet) =
        NodeTransfer(
            rs.getInt(1),
            "${rs.getString(2).replace(" +$".toRegex(),"")} ${rs.getString(3).replace(" +$".toRegex(),"")}",
            "${rs.getString(4).replace(" +$".toRegex(),"")} ${rs.getString(5).replace(" +$".toRegex(),"")}",
            rs.getString(6)
        )
}

/**
 * The Key type maintained by the [PersonParser].
 *
 * @property firstName of the Person.
 * @property lastName of the Person.
 */
class Person(
    var firstName: String,
    var lastName: String
): Comparable<Person>, Serializable
{
    override fun compareTo(other: Person) = comparator.compare(this, other)

    override fun equals(other: Any?): Boolean {
        if (other == null)
            return false
        if (other.javaClass != this.javaClass)
            return false
        return compareTo(other as Person) == 0
    }

    override fun toString() = "$firstName $lastName"

    companion object {
        val comparator = compareBy(Person::firstName, Person::lastName)
        fun getMin() =
            Person("A", "A")
        fun getMax() =
            Person("z".padEnd(30,'z'), "z".padEnd(30,'z'))
    }
}