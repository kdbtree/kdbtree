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

class AverageInt: Function<Int, Pair<Long, Long>, Long> {




   /* v -> 3: a value of the DB-Tree
      output -> (c: 1, s: 3): being Pair.first the count of values and Pair.second their arithmetic sum
    */
    override fun g(v: Int?): Pair<Long, Long> = Pair(1L,v!!.toLong())

    override fun f(aggList: List<Pair<Long, Long>?>): Pair<Long, Long>? = aggList.reduce{temp, next-> Pair(temp!!.first+next!!.first,temp.second+next.second)}


    override fun h(a: Pair<Long, Long>?) : Long = a!!.second / a.first

    //s="(1, 3)"
    //override fun parseA(s: String) : Pair<out Int, out Int> = Pair(${s.get(1).toInt()}, ${s.get(3).toInt()})

    override fun parseA(s: String) : Pair<Long, Long>{
        val pairvalue = s.replace("^\\(|\\)$".toRegex(), "").split(", ")
        return Pair(pairvalue[0].toLong(), pairvalue[1].toLong())
    }
    override fun parseV(s: String) : Int = s.toInt()

    override fun identity(): Pair<Long, Long>? = Pair(0L,0L)

}



