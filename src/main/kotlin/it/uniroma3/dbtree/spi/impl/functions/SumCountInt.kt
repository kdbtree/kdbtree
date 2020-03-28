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

package it.uniroma3.dbtree.spi.impl.functions

import it.uniroma3.dbtree.spi.Function

class SumCountInt : Function<Int, Pair<Long, Long>, Pair<Long, Long>> {

    override fun g(v: Int?): Pair<Long, Long>? =  Pair(v!!.toLong(), 1L)


    override fun f(aggList: List<Pair<Long, Long>?>): Pair<Long, Long>? = aggList.reduce{temp, next-> Pair(temp!!.first+next!!.first,temp.second+next.second)}

    override fun h(a: Pair<Long, Long>?): Pair<Long, Long>? = a

    override fun identity(): Pair<Long, Long>? = Pair(0L, 0L)


    override fun parseA(s: String): Pair<Long, Long>? {
        val x = s.drop(1).dropLast(1).split(",").map { it.trim() }
        return Pair(x[0].toLong(), x[1].toLong())
    }

    override fun parseV(s: String) = s.toInt()
}