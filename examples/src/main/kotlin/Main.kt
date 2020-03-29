@file:JvmName("Main")


import com.github.javafaker.Faker
import kotlin.math.abs
import it.uniroma3.dbtree.DBTree
import it.uniroma3.dbtree.spi.impl.connectors.SQLConnector
import it.uniroma3.dbtree.spi.impl.functions.AverageInt
import it.uniroma3.dbtree.spi.impl.functions.HashInt
import it.uniroma3.dbtree.spi.impl.functions.MaxInt
import it.uniroma3.dbtree.spi.impl.functions.SumCountInt
import it.uniroma3.dbtree.spi.impl.keyParsers.*
import java.util.*
import kotlin.collections.ArrayList
import kotlin.random.Random


const val dbName = "dbtree"
const val tableName = "dbtree"

/**
 * Some examples of usage of the kdbtree library.
 *
 * All the following example use the already implemented SPIs that are provided with the library (version >= 0.2).
 *
 */


fun main(args: Array<String>){
    val urlConn = args[0]
    val userName = args[1]
    val password = args[2]

    /**
     * Example of usage of a dbtree associated to a simple key.
     * Key type is simply an integer.
     * Does not admit groupBy aggregate range query.
     *
     */
    simpleKeyExample(urlConn,userName,password)


    /**
     * Example of usage of a dbtree associated to a simple key identified by two elements.
     * Key type is Person and it is identify by firsName and lastName.
     * Does not admit groupBy aggregate range query.
     */
    simpleKeyExampleTwoElements(urlConn,userName,password)


    /**
     * Example with a groupBy key.
     * This Key type is composed by two attributes, formally k = (x/y).
     * Admits groupBy aggregate range query.
     *
     * In this example the k represents the employees of a company.
     * The x part of the key is the name of the department and the y part is a Person.
     *
      */


    groupByKeyExample(urlConn,userName,password)

    /**
     * Example of the usage of an authenticated dbtree.
     * Aggregation function is an hashing function.
     *
     * This represent an efficient implementation of an Authenticated Data Structure
     * stored in a database.
     *
     */

      authenticatedDBTreeExample(urlConn,userName,password)
}


fun simpleKeyExample(urlConn: String,userName: String, password: String){
    println("--------------------------------------")
    println("// ---------- SIMPLE KEY ---------- //")
    println("--------------------------------------")
    println("Decomposable aggregation function: SumCount class (PartialSum, ElemsCount).")

    /**
     * Initialization of the dbtree
     */
    val kp = IntParser()
    val dbtree = DBTree(
        SumCountInt(),
        SQLConnector(urlConn,dbName,userName, password, tableName, kp)
    )



//    -----------------------------
//    INSERT BATCH (for population)
//    -----------------------------
    println("Populating the DB-tree with pairs having integer keys from 1 to 10.000 and values v_i == k_i.")
    val input = ArrayList<Pair<Int, Int>>()
    for (i in 1 .. 10000) {
        input.add(Pair(i, i))
    }


    dbtree.insertBatch(input)




//    -----------------------------
//    RANGE QUERIES
//    -----------------------------

    // Define a range
    val k1 = 10
    val k2 = 1000

    // consulting the whole dbtree (infinite range)
    val totAggVal = dbtree.rangeQuery(Pair(kp.getMin(), kp.getMax()))

    // consulting a dbtree portion (finite range)
    val rangeQueryResult = dbtree.rangeQuery(Pair(k1, k2))


    println("Total aggregate Value = $totAggVal")
    println("Range [$k1,$k2] aggregate value = ${rangeQueryResult}")

//    -----------------------------
//    DELETION
//    -----------------------------
    val keyToDelete = 5000


    dbtree.delete(keyToDelete)

    // consulting the whole dbtree (infinite range)
    val resDel = dbtree.rangeQuery(Pair(Int.MIN_VALUE, Int.MAX_VALUE))


    println("Deleted key: $keyToDelete. Actual aggregated total ($resDel) lacks ($keyToDelete) from original one ($totAggVal).")

//    -----------------------------
//    INSERTION
//    -----------------------------
    val keyToInsert = 5000

    dbtree.insert(Pair(keyToInsert, keyToInsert))

    // consulting the whole dbtree (infinite range)
    val resIns = dbtree.rangeQuery(Pair(kp.getMin(), kp.getMax()))

    println("Inserted key: $keyToInsert. Actual aggregated total ($resIns) has been restored to the original one ($totAggVal).")

//    -----------------------------
//    UPDATE
//    -----------------------------

    val keyToUpdate = 2500
    val newValue = 1000

    val expDiff = abs(keyToUpdate - newValue)


    dbtree.update(Pair(keyToUpdate, newValue))


    // consulting the whole dbtree (infinite range)
    val resUpd = dbtree.rangeQuery(Pair(kp.getMin(), kp.getMax()))


    println("Updated key: $keyToUpdate, with value: $newValue. " +
            "Actual aggregated total ($resUpd) should differ ($expDiff) from to the original one ($totAggVal).")

    dbtree.close()

}



fun simpleKeyExampleTwoElements(urlConn: String,userName: String, password: String){
    println("--------------------------------------")
    println("// ---------- SIMPLE KEY ---------- //")
    println("// ------- with two elements ------ //")
    println("--------------------------------------")
    println("Decomposable aggregation function: MaxInt class.")
    println("Use case of the example: Which is the maxi age in a range of Persons")

    /**
     * Initialization of the dbtree
     */
    val kp = PersonParser()
    val dbtree = DBTree(
        MaxInt(),
        SQLConnector(urlConn,dbName,userName, password, tableName, kp)
    )



//    -----------------------------
//    INSERT BATCH (for population)
//    -----------------------------
    println("Populating the DB-tree with 1.000 random Person with random name and age (from 18 to 70)")
    val n = 1000
    val input = randomPersons(n)

    dbtree.insertBatch(input)




//    -----------------------------
//    RANGE QUERIES
//    -----------------------------

    // Define a random range of Persons
    var k1 = generatePerson()
    var k2 = generatePerson()

    if(k1 > k2){
        val tmp = k2
        k2 = k1
        k1 = tmp
    }


    // consulting the whole dbtree (infinite range)
    val totAggVal = dbtree.rangeQuery(Pair(kp.getMin(), kp.getMax()))

    // consulting a dbtree portion (finite range)
    val rangeQueryResult = dbtree.rangeQuery(Pair(k1, k2))


    println("Total aggregate Value = $totAggVal")
    println("Range [$k1,$k2] aggregate value = ${rangeQueryResult}")

//    -----------------------------
//    DELETION
//    -----------------------------
    val keyToDelete = input[Random.nextInt(0,n)].first


    dbtree.delete(keyToDelete)


    println("Deleted key: $keyToDelete.")

//    -----------------------------
//    INSERTION
//    -----------------------------
    val keyToInsert = randomPersons(1).first()

    dbtree.insert(keyToInsert)


    println("Inserted key: $keyToInsert.")

//    -----------------------------
//    UPDATE
//    -----------------------------

    val pair = input[Random.nextInt(0,n)]

    val keyToUpdate = pair.first
    val newAge = pair.second + 1


    dbtree.update(Pair(keyToUpdate, newAge))



    println("Updated key: $keyToUpdate, with value: $newAge.")

    dbtree.close()

}


/**
 * Auxiliary function to create n Persons with random name and age
 */

fun randomPersons(n : Int) : List<Pair<Person,Int>>{
    val list = mutableMapOf<Person,Int>()
    while (list.size<n){
        list[generatePerson()] = Random.nextInt(18,70)
    }
    return list.map { Pair(it.key,it.value) }
}


fun generatePerson() : Person{
    val f = Faker()
    val fn = f.name().firstName().trim()
    val ln = f.name().lastName().replace("'", "").trim()
    return Person(fn,ln)
}


fun groupByKeyExample(urlConn: String,userName: String, password: String){

    println("---------------------------------------")
    println("// Group By KEY (group by admitted) //")
    println("// --------- k = ( x / y) --------- //")
    println("------------ x : String --------------")
    println("------------ y : Person --------------")
    println("Decomposable aggregation function: AverageInt class.")
    println("Use case of the example: The average of the salary, groupBy department.")


    val kp = DepartmentEmployeesParser()
    val dbtree = DBTree(AverageInt(),
        SQLConnector(urlConn,dbName,userName, password, tableName, kp))

//    -----------------------------
//    INSERT BATCH (for population)
//    -----------------------------
    println("Populating the DB-tree with goupBy key:")
    println("d different departments")
    println("with a random number of employees (from 1 to n), with a random salary (from 1000 to 10000)")
    val d = 5
    val n = 500
    val input = createDepartmentsEmployees(d,n)


    dbtree.insertBatch(input)
//    -----------------------------
//    RANGE QUERIES
//    -----------------------------
    val totAggVal = dbtree.rangeQuery(Pair(kp.getMin(), kp.getMax()))
    println("Total aggregate Value = $totAggVal")  // consulting the whole dbtree (infinite range)

//    -----------------------------
//    GROUP BY
//    -----------------------------


    val minPersonRange = Person.getMin()
    val maxPersonRange = Person.getMax()
    val listMin = listOf(minPersonRange.firstName,minPersonRange.lastName)
    val listMax= listOf(maxPersonRange.firstName,maxPersonRange.lastName)

    val groupByResultAll =  (dbtree.groupBy(Pair(listMin, listMax))).sortedBy { it.first.toString()  }
    println("GroupBy total aggregate values for each department: ")
    groupByResultAll.forEach { println(it) }

    var e1 = generatePerson()
    var e2 = generatePerson()

    if(e1 > e2){
        val tmp = e2
        e2 = e1
        e1 = tmp
    }

    val listE1 = listOf(e1.firstName,e1.lastName)
    val listE2 = listOf(e2.firstName,e2.lastName)

    val groupByResult = (dbtree.groupBy(Pair(listE1, listE2))).sortedBy { it.first.toString()}
    println("GroupBy range query where the Employees are in the range of \n [$e1,$e2]: ")
    groupByResult.forEach { println(it) }

    println()

    dbtree.close()
}

/**
 * Auxiliary function to create d different Departments with a different number of employees
 * with a random salary from 1.000 to 10.000
 */


fun createDepartmentsEmployees(d: Int, n: Int): List<Pair<DepartmentEmployee,Int>>{
    val list = mutableListOf<Pair<DepartmentEmployee,Int>>()

    (1..d).forEach {
        val nameDep = generateDepartmentName().replace(",","")
        val listTemp = List(Random.nextInt(1,n)) {
            Pair(
                DepartmentEmployee(nameDep,generatePerson()),
                Random.nextInt(1,10)*1000
            )
        }
        list.addAll(listTemp)

    }
    return list
}

fun generateDepartmentName() : String {
    var departName: String
    do{
        departName = Faker().commerce().department()
    }while (departName.length >= 50) //In DepartmentEmployeesParser we define the name of a department as a CHAR(50)
     return departName
}




fun authenticatedDBTreeExample(urlConn: String,userName: String, password: String){
    println("-----------------------------------------")
    println(" -------- AUTHENTICATED DB-TREE -------- ")
    println("-----------------------------------------")
    println("Decomposable aggregation function: HashInt class.")
    println("---------(execute a SHA256 on Int)-------")


    val aggFunc= HashInt()
    val kp = IntParser()
    val dbtree = DBTree(aggFunc,
        SQLConnector(urlConn,dbName,userName, password, tableName, kp))

//    -----------------------------
//    INSERT BATCH (for population)
//    -----------------------------
    println("Populating the DB-tree with pairs having integer keys from 1 to 10.000 and values v_i == k_i.")
    val input = ArrayList<Pair<Int, Int>>()
    for (i in 1 .. 10000) {
        input.add(Pair(i, i))
    }

    dbtree.insertBatch(input)

//    -----------------------------
//    RANGE QUERIES
//    -----------------------------
    val totAggVal = dbtree.rangeQuery(Pair(kp.getMin(), kp.getMax()))
    println("Root Hash (total aggregate value) = $totAggVal")  // consulting the whole dbtree (infinite range)


//    -----------------------------
//    DELETION
//    -----------------------------
    val keyToDelete = 5000
    dbtree.delete(keyToDelete)
    val resDel = dbtree.rangeQuery(Pair(kp.getMin(), kp.getMax()))  // consulting the whole dbtree (infinite range)
    println("Deleted key: $keyToDelete. New root hash: $resDel.")

//    -----------------------------
//    INSERTION
//    -----------------------------
    val keyToInsert = 5000
    dbtree.insert(Pair(keyToInsert, keyToInsert))
    val resIns = dbtree.rangeQuery(Pair(kp.getMin(), kp.getMax()))  // consulting the whole dbtree (infinite range)
    println("Re-inserted pair: ($keyToInsert, $keyToInsert). Root Hash is restored: $resIns.")

//    -----------------------------
//    AUTHENTICATED QUERY
//    -----------------------------
    val queriedKey = 5000
    println("Authenticated query for key: $queriedKey. \n" +
            "Proof structure: substitute null value with digest computed " +
            "from previous node (queried value for 1st node).")

    val proof = dbtree.authenticatedQuery(queriedKey)
    println("Proof: $proof")
    println("check proof correctness against roothash : $resIns")

    /*
    *
    * Calculating the root hash using the received value and its proof
    *
    *
     */
    val returnV = proof.first
    val pr = proof.second
    var calcRoot = aggFunc.g(returnV)

    for(n in pr){
        var temp = n.map { e -> if(e=="null") calcRoot else e }
        calcRoot = aggFunc.f(temp)
    }
    calcRoot = aggFunc.h(calcRoot)


    println("the two root hashes are equal = ${calcRoot==resIns} \ncalculated roothash: $calcRoot")



    dbtree.close()
}

