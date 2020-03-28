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
import java.security.MessageDigest

class HashInt: Function<Int, String, String> {

    val ed = MessageDigest.getInstance("SHA-256")

    override fun g(v: Int?): String? {
        val encodedhashK = v.toString().toByteArray()

        val digest= ed.digest(encodedhashK)

        return digest.fold("", { str, it -> str + "%02x".format(it) })
    }

    override fun f(aggList: List<String?>): String? {

        val h = aggList.joinToString("")

        val encodedhashK = h.toByteArray()

        val digest= ed.digest(encodedhashK)

        return digest.fold("", { str, it -> str + "%02x".format(it) })
    }

    override fun h(a: String?): String {
        return a.toString()

    }

    override fun identity() = ""

    override fun parseA(s: String) : String = s

    override fun parseV(s: String) :Int = s.toInt()

}