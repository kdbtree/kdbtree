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
import it.uniroma3.dbtree.spi.KeyParserGroupBy
import java.io.Serializable
import java.sql.ResultSet

/**
 * Example of [KeyParserGroupBy] class, managing a groupBy key: [DepartmentEmployee].
 * Group: the name of the department.
 * Least significant key's component: [Person].
 *
 */
class DepartmentEmployeesParser : KeyParserGroupBy<DepartmentEmployee>(){

    //We can use the PersonParse to easily implements functions as lspltDB
    private val personParser = PersonParser()


    override val mostSignificantDefinition = listOf(Pair("department","CHAR(50) NOT NULL"))
    override val leastSignificantDefinition = personParser.keyColumnsDefinitionDB

    override fun getMax() =
        DepartmentEmployee.getMax()

    override fun getMin() =
        DepartmentEmployee.getMin()

    override fun bind(x: List<Any>, y: List<Any>) = DepartmentEmployee(
        x[0].toString(),
        Person(y[0].toString(),y[1].toString())
    )

    override fun group(s: String): List<Any> {
        return listOf(s.drop(1).dropLast(1).split(" // ")[0])
    }

    override fun lsp_ltDB(columnName: List<String>, y: List<Any>) =
        personParser.ltDB(columnName,Person(y[0].toString(),y[1].toString()))


    override fun lsp_leqDB(columnName: List<String>, y: List<Any>) =
        personParser.leqDB(columnName,Person(y[0].toString(),y[1].toString()))


    override fun lsp_gtDB(columnName: List<String>, y: List<Any>) =
        personParser.gtDB(columnName,Person(y[0].toString(),y[1].toString()))

    override fun lsp_geqDB(columnName: List<String>, y: List<Any>) =
        personParser.geqDB(columnName,Person(y[0].toString(),y[1].toString()))


    override fun most_neqDB(columnNameMin: List<String>, columnNameMax: List<String>) =
        "${columnNameMin[0]} <> ${columnNameMax[0]}"



    override fun parse(s: String): DepartmentEmployee {
        val split = s.drop(1).dropLast(1).split(" // ")
        return DepartmentEmployee(
            split[0],
            personParser.parse(split[1])
        )
    }

    override fun ltDB(columnName: List<String>, k: DepartmentEmployee) =
        "(${columnName[0]} < '${k.depName}' OR (${columnName[0]} = '${k.depName}' AND ${personParser.ltDB(columnName.drop(1),k.info)}))"


    override fun leqDB(columnName: List<String>, k: DepartmentEmployee) =
        "(${columnName[0]} < '${k.depName}' OR (${columnName[0]} = '${k.depName}' AND ${personParser.leqDB(columnName.drop(1),k.info)}))"


    override fun gtDB(columnName: List<String>, k: DepartmentEmployee) =
        "(${columnName[0]} > '${k.depName}' OR (${columnName[0]} = '${k.depName}' AND ${personParser.gtDB(columnName.drop(1),k.info)}))"


    override fun geqDB(columnName: List<String>, k: DepartmentEmployee) =
        "(${columnName[0]} > '${k.depName}' OR (${columnName[0]} = '${k.depName}' AND ${personParser.geqDB(columnName.drop(1),k.info)}))"


    override fun eqDB(columnName: List<String>, k: DepartmentEmployee) =
        "(${columnName[0]} = '${k.depName}' AND ${personParser.eqDB(columnName.drop(1),k.info)})"

    override fun toDB(k: DepartmentEmployee) =
     "'${k.depName}', ${personParser.toDB(k.info)}"

    override fun toNodeTransfer(rs: ResultSet) =
        NodeTransfer(
            rs.getInt(1),
            "(${rmLastWhiteSpaces(rs.getString(2))} // ${Person(rmLastWhiteSpaces(rs.getString(3)), rmLastWhiteSpaces(rs.getString(4)))})",
            "(${rmLastWhiteSpaces(rs.getString(5))} // ${Person(rmLastWhiteSpaces(rs.getString(6)), rmLastWhiteSpaces(rs.getString(7)))})",
            rs.getString(8)
        )

    private fun rmLastWhiteSpaces(s: String) = s.replace(" +$".toRegex(),"")
}

/**
 * The Key type maintained by the [DepartmentEmployeesParser].
 *
 * @property depName The department name where the employee works.
 *
 * @property info The information about the employ. Provided as [Person]
 *
 */
class DepartmentEmployee(
    var depName : String,
    var info : Person
): Comparable<DepartmentEmployee>, Serializable
{
    override fun compareTo(other: DepartmentEmployee) = comparator.compare(this, other)

    override fun equals(other: Any?): Boolean {
        if (other == null)
            return false
        if (other.javaClass != this.javaClass)
            return false
        return compareTo(other as DepartmentEmployee) == 0
    }

    override fun toString() = "($depName // $info)"

    companion object {
        val comparator = compareBy(DepartmentEmployee::depName, DepartmentEmployee::info)
        fun getMin() =
            DepartmentEmployee("A", PersonParser().getMin())
        fun getMax() =
            DepartmentEmployee("z".padEnd(50,'z'), PersonParser().getMax())
    }

}