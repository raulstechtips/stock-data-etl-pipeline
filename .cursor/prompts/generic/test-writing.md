---
name: "Generic Test Writing Template"
category: "testing"
description: "Write comprehensive tests for Django-integrated microservices (background workers, CLI tools, scheduled jobs, PySpark jobs)"
version: "1.0"
variables:
  - name: "component_type"
    description: "Type of component: 'background-worker' (Celery/RQ), 'cli-tool', 'scheduled-job', 'pyspark-job'"
    required: true
  - name: "component_name"
    description: "Component to test (e.g., 'SendEmailTask', 'ExportReportCommand', 'EventAggregationJob')"
    required: true
  - name: "coverage_areas"
    description: "Specific areas to cover (e.g., 'error handling, retries, Django ORM integration, idempotency')"
    required: false
---

# Generic Test Writing Template

## Context

This template guides writing comprehensive tests for Django-integrated microservices including background workers, CLI tools, scheduled jobs, and PySpark data transformation jobs. Tests should validate core logic, Django ORM integration, error handling, and containerized execution.

### Component Types
- **Background Workers**: Celery tasks, RQ workers, async job processors
- **CLI Tools**: Django management commands, standalone scripts
- **Scheduled Jobs**: Cron jobs, periodic tasks, schedulers
- **PySpark Jobs**: Data transformation, ETL pipelines, batch processing

### Test Coverage Goals
- Happy path (normal operation)
- Django ORM integration (queries, transactions, connections)
- Error handling and retry logic
- Edge cases (empty data, missing records, race conditions)
- Idempotency (for workers and scheduled jobs)
- Performance (for large datasets)

## Instructions

### Step 1: Analyze the Component
- Component type: `{{component_type}}`
- Identify component to test: `{{component_name}}`
- Review coverage areas: `{{coverage_areas}}`
- Read the implementation code
- Identify test scenarios based on component type

### Step 2: Setup Test Environment

**For Background Workers:**
- Use Celery test utilities or mock task execution
- Set up test database with Django TestCase
- Configure eager task execution for tests
- Mock external services (email, S3, APIs)

**For CLI Tools:**
- Use Django's `call_command()` helper
- Capture stdout/stderr for assertions
- Set up test database with fixtures
- Mock file system operations if needed

**For Scheduled Jobs:**
- Mock scheduling/locking mechanisms
- Test with frozen time (using freezegun)
- Set up test database state
- Mock distributed locks (cache)

**For PySpark Jobs:**
- Use local Spark session for tests
- Create small test dataframes
- Mock S3/HDFS reads/writes
- Test Django ORM integration separately

### Step 3: Write Test Cases

Follow the Arrange-Act-Assert pattern:
1. **Arrange**: Set up test data and preconditions
2. **Act**: Execute the code under test
3. **Assert**: Verify expected outcomes

Use naming convention: `test_<method>_<scenario>_<expected_result>`

### Step 4: Cover Component-Specific Scenarios

**For Background Workers:**
- Task executes successfully with valid input
- Task retries on transient failures
- Task fails gracefully on permanent errors
- Task is idempotent (safe to run multiple times)
- Django ORM connections handled properly
- Task handles deleted/missing database records

**For CLI Tools:**
- Command runs successfully with valid args
- Command validates input arguments
- Command handles large datasets efficiently (batching)
- Command reports progress correctly
- Command handles file I/O errors
- Command uses transactions properly

**For Scheduled Jobs:**
- Job executes successfully
- Job prevents concurrent execution (locking)
- Job is idempotent
- Job handles partial failures
- Job tracks last successful run
- Job cleans up resources

**For PySpark Jobs:**
- Job processes data correctly
- Job handles schema mismatches
- Job writes to Django ORM efficiently
- Job handles empty datasets
- Job manages Spark session lifecycle
- Job handles partition failures

### Step 5: Add Integration Tests
- Test with real Django database
- Test in containerized environment (if applicable)
- Test with realistic data volumes
- Verify logging output
- Test monitoring/metrics integration

### Step 6: Review Test Quality
- Descriptive test names
- Clear docstrings
- No test interdependencies
- Fast execution (use mocking where appropriate)
- Deterministic results
- Meaningful coverage

## Constraints

- **Arrange-Act-Assert**: Follow this pattern consistently
- **Descriptive Names**: Test names should describe what they verify
- **Isolation**: Tests should not depend on each other
- **Django TestCase**: Use Django's test framework for ORM tests
- **Mocking**: Mock external services and slow operations
- **Fast Execution**: Keep tests fast with minimal database operations
- **Container Testing**: Validate containerized execution where applicable

## Expected Output

### Test File Structure
```python
from django.test import TestCase
from unittest.mock import patch, Mock
# Component-specific imports

class ComponentNameTests(TestCase):
    def setUp(self):
        """Set up test data and mocks."""
        pass
    
    def test_happy_path_scenario(self):
        """Test normal operation with valid input."""
        # Arrange
        # Act
        # Assert
        pass
    
    def test_error_handling_scenario(self):
        """Test component handles errors gracefully."""
        # Arrange
        # Act
        # Assert
        pass
    
    def test_django_orm_integration(self):
        """Test Django ORM operations work correctly."""
        # Arrange
        # Act
        # Assert
        pass
    
    # More test methods...
```

### Coverage Summary
- Number of test cases written
- Scenarios covered (happy path, edge cases, errors, ORM integration)
- Component-specific concerns addressed
- Performance tests (if applicable)
- Integration test coverage

### Quality Checklist
- [ ] Descriptive test names
- [ ] Clear docstrings
- [ ] Arrange-Act-Assert pattern
- [ ] Django ORM integration tested
- [ ] Error handling tested
- [ ] Idempotency tested (for workers/jobs)
- [ ] Mocking used appropriately
- [ ] Fast execution
- [ ] All tests pass

