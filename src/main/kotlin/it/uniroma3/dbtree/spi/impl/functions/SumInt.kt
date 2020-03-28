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

class SumInt: Function<Int, Long, Long> {

    override fun g(v: Int?) = v?.toLong()

    override fun f(aggList: List<Long?>) : Long? {
        var res = 0L
        for(a in aggList)
            if(a!=null) res += a
        return res
    }

    override fun h(a: Long?) = a

    override fun identity() = 0L

    override fun parseA(s: String) = s.toLong()

    override fun parseV(s: String) = s.toInt()

}
