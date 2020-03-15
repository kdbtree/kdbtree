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

import java.io.Serializable

/**
 * Interface for the decomposable aggregation function α ≔ 〈f, g, h〉 supported by the DB-tree.
 *
 * The decomposable aggregation function α is defined such that α(v₁, ... , vₙ) = h(f(g(v₁), ... , g(vₙ))), where:
 * g: V -> A
 * f: A₁, ..., Aₙ -> A  (core aggregation function)
 * h: A -> D
 *
 * @param V the type of the atomic values stored in the DB-tree.
 * @param A the type of the aggregate values.
 * @param D the final result type, calculated over the final aggregate value.
 */
interface Function<V: Serializable, A: Serializable, D> {

    /**
     * Computes the aggregate value from an atomic value.
     *
     * E.g.: in the case of arithmetic mean,
     * v = 3 (a value of the DB-tree) -> g(v) = <1,3> (the count of elements and corresponding sum).
     *
     * @param v the atomic value.
     * @return the corresponding aggregate value.
     */
    fun g(v: V?) : A?

    /**
     * Function's component for the combination of aggregate values.
     *
     * E.g.: in the case of arithmetic mean,
     * a1,a2 = <c1,s1>, <c2,s2> (the count and the sum of each aggregate value)
     * f(a1,a2) = <(c1+c2),(s1+s2)> (the combined aggregate value).
     *
     * @param aggList the list of aggregate values to combine.
     * @return the combined aggregate value.
     */
    fun f(aggList: List<A?>) : A?

    /**
     * Generates a final result from an aggregate value.
     *
     * E.g.: in the case of arithmetic mean,
     * a = <c,s> (the element count and their sum)
     * h(a) = s/c (the arithmetic mean itself).
     *
     * @param a the aggregate value.
     * @return the final result.
     */
    fun h(a: A?) : D?

    /**
     * Returns the identity aggregate value for this aggregation function.
     *
     * I.e.: a0 that value such that -> f(a1,a0) = a1.
     *
     * @return the identity aggregate value.
     */
    fun identity() : A?

    /**
     * Parser for the aggregate value type.
     *
     * @param s the string aggregate value.
     * @return the corresponding [A] representation.
     */
    fun parseA(s: String) : A?

    /**
     * Parser for the atomic value type.
     *
     * @param s the string atomic value.
     * @return the corresponding [C] representation.
     */
    fun parseV(s: String) : V?

}