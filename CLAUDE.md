# CLAUDE.md

## Commands

### Build and Test
- Build: `./gradlew build`
- Clean build: `./gradlew clean build`
- Run tests: `./gradlew test`
- Run a single test: `./gradlew test --tests "io.github.panghy.taskqueue.SomeTest"`

### Code Quality
- Apply code formatting: `./gradlew spotlessApply`
- Check code formatting: `./gradlew spotlessCheck`
- Generate coverage report: `./gradlew jacocoTestReport`
- Check coverage thresholds: `./gradlew jacocoTestCoverageVerification`

### Publishing
- Publish snapshot: `./gradlew publishToSonatype`
- Publish release: `./gradlew publishAndReleaseToMavenCentral`

## Architecture

This is a distributed task queue library backed by FoundationDB. The key architectural components:

### Core Components

1. **TaskQueueConfig** - Configuration with type parameters for task keys and data
   - Uses builder pattern for construction
   - Configurable TTL, max attempts, and throttling
   - Pluggable serializers for keys and data

2. **Protobuf Messages** (in `src/main/proto/task.proto`)
   - `Task` - Represents a task in the queue with claim tracking
   - `TaskKey` - Contains actual task data and execution parameters
   - `TaskKeyMetadata` - Tracks version and claim information

### Task Semantics
- FIFO processing with visibility time support
- At-most-once execution per task key
- Task versioning with automatic latest version execution
- Configurable TTL for worker lease duration
- Throttling to prevent rapid re-execution

## Code Style
- Uses Palantir Java Format via Spotless
- 80% line coverage requirement
- 70% branch coverage requirement
- Protobuf generated classes are excluded from coverage