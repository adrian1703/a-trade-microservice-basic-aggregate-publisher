plugins {
    kotlin("jvm") version "2.2.10"
    kotlin("plugin.allopen") version "2.2.10"
    id("org.springframework.boot") version "3.3.1"
    id("io.spring.dependency-management") version "1.1.4"
}

repositories {
    mavenCentral()
    mavenLocal()
}


dependencies {
    implementation("adrian.kuhn:a-trade-microservice-runtime-api:0.0.1")
    implementation("net.jcip:jcip-annotations:1.0")
    implementation("org.springframework:spring-webflux")
    implementation("org.apache.avro:avro:1.12.0")
    implementation("org.apache.kafka:kafka-clients:4.1.0")

    // openapi stuff
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
   // implementation("javax.validation:validation-api")
    implementation("javax.annotation:javax.annotation-api:1.3.2")
    implementation("org.springdoc:springdoc-openapi-ui:1.6.8")
    // openapi stuff ende
    implementation("com.opencsv:opencsv:5.9")

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
}

group = "adrian.kuhn"
version = "0.0.1"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<Test> {
//    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    jvmArgs("--add-opens", "java.base/java.lang=ALL-UNNAMED")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.set(listOf("-Xannotation-default-target=param-property"))
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21
        javaParameters = true
    }
}