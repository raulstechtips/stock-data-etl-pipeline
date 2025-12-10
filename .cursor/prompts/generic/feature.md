---
name: "Generic Feature Implementation Template"
category: "feature"
description: "Structured approach to implementing new features in Django-integrated microservices (background workers, CLI tools, scheduled jobs, PySpark jobs)"
version: "1.0"
variables:
  - name: "component_type"
    description: "Type of component: 'background-worker' (Celery/RQ), 'cli-tool', 'scheduled-job', 'pyspark-job'"
    required: true
  - name: "feature_name"
    description: "Name of the feature to implement (e.g., 'Daily User Report Generator', 'Async Email Queue')"
    required: true
  - name: "requirements"
    description: "Detailed feature requirements and specifications"
    required: true
  - name: "acceptance_criteria"
    description: "How to verify the feature works correctly"
    required: true
  - name: "integration_points"
    description: "How this component integrates with Django ORM, other services, or data sources"
    required: false
---

# Generic Feature Implementation Template

## Context

This template guides feature implementation in Django-integrated microservices including background workers, CLI tools, scheduled jobs, and PySpark data transformation jobs. Features should follow containerized deployment patterns, maintain Django ORM best practices, and integrate cleanly within the monorepo architecture.

### Component Types
- **Background Workers**: Celery tasks, RQ workers, async job processors
- **CLI Tools**: Django management commands, standalone scripts
- **Scheduled Jobs**: Cron jobs, periodic tasks, schedulers
- **PySpark Jobs**: Data transformation, ETL pipelines, batch processing

### Implementation Flow
1. **Plan**: Requirements analysis and architecture design
2. **Implement**: Core logic with Django ORM integration
3. **Configure**: Container setup, dependencies, environment
4. **Test**: Unit tests, integration tests, container tests
5. **Document**: Usage, deployment, monitoring

## Instructions

### Step 1: Planning & Design
- Component type: `{{component_type}}`
- Analyze requirements: `{{feature_name}}`
- Review specifications: `{{requirements}}`
- Define acceptance criteria: `{{acceptance_criteria}}`
- Identify integration points: `{{integration_points}}`
- Design data flow and dependencies
- Consider failure modes and retry strategies
- Plan containerization approach

### Step 2: Implement Core Logic

**For Background Workers (Celery/RQ):**
- Create task function with proper decorators
- Configure retry policy and error handling
- Handle Django ORM connections properly
- Implement idempotency if needed
- Add task status tracking
- Use appropriate task queues

**For CLI Tools (Management Commands):**
- Create management command class
- Define command arguments and options
- Implement progress reporting
- Handle large datasets efficiently (batching)
- Add transaction management
- Provide clear success/error messages

**For Scheduled Jobs:**
- Implement job entry point
- Add locking mechanism to prevent concurrent runs
- Track last successful run timestamp
- Implement state persistence if needed
- Handle partial failures and resume capability
- Configure schedule (cron expression, etc.)

**For PySpark Jobs:**
- Initialize Spark session with appropriate config
- Design dataframe transformations
- Handle Django ORM writes efficiently (bulk operations)
- Implement partition-aware processing
- Configure memory and executor settings
- Add checkpointing for long-running jobs

### Step 3: Django ORM Integration
- Use appropriate connection handling for component type
- Implement bulk operations for efficiency
- Add transaction boundaries where appropriate
- Handle connection pooling in multi-threaded/distributed contexts
- Use select_related/prefetch_related for query optimization
- Close connections explicitly in long-running processes

### Step 4: Error Handling & Logging
- Add structured logging at key decision points
- Implement comprehensive error handling
- Configure retry strategies with exponential backoff
- Add monitoring hooks (metrics, alerts)
- Log inputs/outputs for debugging
- Handle graceful shutdown

### Step 5: Containerization
- Define Dockerfile if new container needed
- Add dependencies to requirements file
- Configure environment variables
- Set up health checks
- Define resource limits (memory, CPU)
- Update docker-compose for local development

### Step 6: Testing
- Write unit tests for core logic
- Add integration tests with Django ORM
- Test in containerized environment
- Verify error handling and retries
- Test with realistic data volumes
- Validate monitoring and logging

### Step 7: Documentation
- Add docstrings to all functions/classes
- Document configuration options
- Provide usage examples
- Document deployment steps
- Add monitoring/alerting guide
- Include troubleshooting tips

## Constraints

- **Django ORM Best Practices**: Proper connection handling, transactions, query optimization
- **Container Compatibility**: Must run in containerized environment
- **Monorepo Integration**: Follow project structure and import patterns
- **Error Handling**: Comprehensive error handling with proper logging
- **Idempotency**: Operations should be idempotent where possible
- **Resource Efficiency**: Optimize memory and CPU usage
- **Monitoring**: Include logging and metrics for observability
- **Testing**: Comprehensive test coverage including integration tests


## Expected Output

### 1. Implementation Files
- Core logic files with proper structure
- Django ORM integration code
- Configuration files (if applicable)

### 2. Container Configuration
- Dockerfile or docker-compose updates
- Environment variable definitions
- Resource limits configuration

### 3. Tests
- Unit tests for core logic
- Integration tests with Django ORM
- Container environment tests
- Edge case coverage

### 4. Documentation
- Docstrings on all functions/classes
- Usage examples and CLI help
- Deployment instructions
- Monitoring and alerting setup
- Troubleshooting guide

### 5. Deployment Artifacts
- Updated dependencies (requirements.txt, etc.)
- Environment configuration
- Scheduling configuration (cron, Celery beat, etc.)
- Monitoring/alerting setup

