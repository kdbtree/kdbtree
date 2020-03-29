
plugins {
    kotlin("jvm")
}



repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("com.github.kdbtree:kdbtree:0.2")
    implementation("com.github.javafaker:javafaker:1.0.2")
    implementation("org.postgresql:postgresql:42.2.10")
    implementation("mysql:mysql-connector-java:8.0.16")
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
}


val virtNetwork = "DB-Tree_net"
val subnetPrefix = "172.21"
val subnet = "$subnetPrefix.0.0/16"

data class ConfigDatabase(val jdbcDriver: String,
                          val jdbcUrlprefix: String,
                          val username: String,
                          val password: String)


data class ConfigContainer(val dockerImageName: String,
                           val databaseInfo: ConfigDatabase,
                           val ip: String,
                           val exposePort: String,
                           val otherRunningOptions: String)

val containerList = listOf(
    ConfigContainer("postgres",ConfigDatabase("org.postgresql.Driver","jdbc:postgresql:","postgres", "postgres"),"$subnetPrefix.1.1", "5432","-e POSTGRES_PASSWORD=postgres"),
    ConfigContainer("mysql",ConfigDatabase("com.mysql.cj.jdbc.Driver","jdbc:mysql:","root", "root"),"$subnetPrefix.1.2","3306","-e MYSQL_ROOT_PASSWORD=root")
)





fun createCommand(s: String) = s.split("\\s+".toRegex())

fun shellCheck(cmd: String, timeout: Long = 5): Int {
    val pb = ProcessBuilder("bash", "-c", cmd)
    val proc = pb.inheritIO().start()
    return proc.waitFor()
}

fun imageExists(name: String) : Boolean {
    return shellCheck("""docker image ls -f "reference=$name" | tail -n -2 | grep "$name" """) == 0
}

fun containerExists(name: String) : Boolean {
    return shellCheck("test $(docker ps -a -f \"name=$name\" --format '{{.Names}}') " +
                "== \"$name\""
    ) == 0
}

fun netExists( ) : Boolean {
    return shellCheck("docker network list " + "| grep -w \"$virtNetwork\"") == 0
}


/**
 *
 * TASKS
 *
**/


val virtualNetUp by tasks.register<Exec>("virtualNetUp") {
    group = "dbtree-virtualNetwork"
    description = "Checks if virtual network $virtNetwork exists in docker, otherwise it creates it"
    commandLine = createCommand("echo 'docker virtual net exist'")
    if(!netExists()){
        commandLine = createCommand("docker network create $virtNetwork --subnet ${subnet}")
    }
}

val virtualNetDown by tasks.register<Exec>("virtualNetDown") {
    group = "dbtree-virtualNetwork"
    description = "Tears down virtual network $virtNetwork (and all machines)"
    commandLine = createCommand("echo 'docker virtual net is down'")
    if(netExists()) {
        commandLine = createCommand("docker network rm $virtNetwork")
    }
    dependsOn(containerList.map { tasks["container-${it.dockerImageName}_Down"] })
}



// A set of tasks for each container
containerList.forEach {
    val currentGroup = "dbtree-${it.dockerImageName}"
    val checkimageDocker by tasks.register<Exec>("checkImage${it.dockerImageName}"){
        group = currentGroup
        description = "Checks if the docker image is present or pull it"
        commandLine = createCommand("echo 'image ${it.dockerImageName} is present'")
        if(!imageExists(it.dockerImageName)){
            commandLine = createCommand("docker pull ${it.dockerImageName}:latest")
        }
    }

    val containerName = "container-${it.dockerImageName}"

    val containerUp by tasks.register<Exec>("${containerName}_UP") {
        group = currentGroup
        description = "run the docker container with image ${it.dockerImageName}"
        commandLine = createCommand("echo 'docker ${containerName} is  already up'")
        if(!containerExists(containerName)){
            commandLine = createCommand("docker run -dit --name $containerName --network $virtNetwork --ip ${it.ip} -p ${it.exposePort}:${it.exposePort} ${it.otherRunningOptions} ${it.dockerImageName}")
        }
        dependsOn(virtualNetUp)
    }

    val containerDown by tasks.register<Exec>("${containerName}_Down") {
        group = currentGroup
        description = "Tears down machine $containerName"
        commandLine = createCommand("echo 'docker container ${containerName} not exist'")
        if(containerExists(containerName)){
            commandLine = createCommand("docker rm -v -f $containerName")
        }
    }

    val Up by tasks.register("${it.dockerImageName}_AllUp") {
        group = currentGroup
        description = "Start all environment for ${containerName}"
        dependsOn(containerUp)
    }

    val Down by tasks.register("${it.dockerImageName}_AllDown") {
        group = currentGroup
        description = "Stop all environment"
        dependsOn(virtualNetDown)
    }
    val runExamples by tasks.register<JavaExec>("${it.dockerImageName}_runExamples"){
        group = currentGroup
        description = "run the main class using ${containerName}"
        dependsOn(containerUp)
        main = "Main"
        val urlConn = "${it.databaseInfo.jdbcUrlprefix}//${it.ip}:${it.exposePort}/"

        args = listOf(urlConn,it.databaseInfo.username,it.databaseInfo.password)
        classpath = sourceSets["main"].runtimeClasspath
    }
}



