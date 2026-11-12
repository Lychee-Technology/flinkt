// The convention plugins and their tests are in build-logic, an included build. A task named on the command line,
// such as `build`, doesn't reach an included build, so the root project's `check` runs build-logic's.
plugins {
    base
}

tasks.check {
    dependsOn(gradle.includedBuild("build-logic").task(":check"))
}
