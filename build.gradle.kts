plugins {
    id("java")
}

group = "com.task.softmotion"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.codehaus.groovy:groovy:3.0.19")
    implementation("org.codehaus.groovy:groovy-xml:3.0.19")
    implementation("org.postgresql:postgresql:42.7.8")
    implementation("org.slf4j:slf4j-simple:2.0.9")
}

tasks.test {
    useJUnitPlatform()
}
