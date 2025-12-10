---
name: "Test Writing Template"
category: "testing"
description: "Write comprehensive tests for Django/DRF applications using TestCase, APITestCase, and pytest patterns"
version: "1.0"
variables:
  - name: "component_name"
    description: "Component to test (e.g., 'TaskService', 'TaskViewSet', 'Task model')"
    required: true
  - name: "test_type"
    description: "Type of test: 'model', 'service', 'api', 'integration', 'unit'"
    required: true
  - name: "coverage_areas"
    description: "Specific areas to cover (e.g., 'validation, edge cases, permissions')"
    required: false
---

# Test Writing Template

## Context

This template guides writing comprehensive tests for Django/DRF applications. Tests should follow the Arrange-Act-Assert pattern, use descriptive names, cover edge cases, and align with the project's testing patterns.

### Testing Framework Choices
- **Django TestCase**: For models, managers, and database-dependent logic
- **DRF APITestCase**: For API endpoints and views
- **pytest**: For unit tests and service layer (if using pytest in project)

### Test Coverage Goals
- Happy path (normal operation)
- Edge cases (boundary conditions, empty inputs)
- Error cases (invalid input, permissions, not found)
- Race conditions (for concurrent operations)
- Transaction rollback (for atomic operations)

## Instructions

### Step 1: Analyze the Component
- Identify component to test: `{{component_name}}`
- Determine test type: `{{test_type}}`
- Review coverage areas: `{{coverage_areas}}`
- Read the implementation code
- Identify test scenarios (happy path, edge cases, errors)

### Step 2: Setup Test Class
- Choose appropriate base class (TestCase, APITestCase, TransactionTestCase)
- Create test fixtures in `setUp()` or use `@classmethod setUpTestData()`
- Use factories or fixtures for test data
- Set up authentication if testing API endpoints

### Step 3: Write Test Cases

For each test method:
1. **Arrange**: Set up test data and preconditions
2. **Act**: Execute the code under test
3. **Assert**: Verify expected outcomes

Follow naming convention: `test_<method>_<scenario>_<expected_result>`

### Step 4: Cover Key Scenarios

**For Models (`test_type: model`)**:
- Field validation
- Model methods
- Custom managers/querysets
- `__str__` representation
- Constraints and unique fields

**For Services (`test_type: service`)**:
- Business logic correctness
- Transaction handling
- Error conditions
- Edge cases
- Side effects (logging, signals)

**For APIs (`test_type: api`)**:
- HTTP methods (GET, POST, PUT, PATCH, DELETE)
- Authentication and permissions
- Request validation
- Response format and status codes
- Pagination and filtering

**For Integration (`test_type: integration`)**:
- End-to-end workflows
- Multiple component interaction
- Database state changes
- External service mocking

### Step 5: Add Edge Cases and Error Tests
- Null/empty inputs
- Boundary values
- Invalid data types
- Permission denied scenarios
- Not found scenarios
- Concurrent access (if applicable)

### Step 6: Review Test Quality
- Descriptive test names
- One assertion focus per test (generally)
- No test interdependencies
- Fast execution
- Deterministic results

## Constraints

- **Arrange-Act-Assert**: Follow this pattern consistently
- **Descriptive Names**: Test names should describe what they verify
- **Isolation**: Tests should not depend on each other
- **Fast**: Keep tests fast by minimizing database operations
- **Deterministic**: Tests should produce same results every run
- **Coverage**: Aim for meaningful coverage, not just high percentages
- **Use Factories**: Prefer factories over manual object creation
- **Clean Assertions**: Use appropriate assertion methods


## Expected Output

### Test File Structure
```python
from django.test import TestCase  # or APITestCase, TransactionTestCase
from rest_framework.test import APITestCase
from rest_framework import status
# ... other imports

class ComponentNameTests(TestCase):  # Descriptive class name
    @classmethod
    def setUpTestData(cls):
        """Set up data for all tests in this class."""
        # Create test data used by all tests
        pass
    
    def setUp(self):
        """Set up before each test method."""
        # Per-test setup
        pass
    
    def test_method_scenario_expected_result(self):
        """Descriptive docstring explaining what this tests."""
        # Arrange
        # ... setup
        
        # Act
        # ... execute
        
        # Assert
        # ... verify
        pass
    
    # More test methods...
```

### Coverage Summary
- Number of test cases written
- Scenarios covered (happy path, edge cases, errors)
- Any uncovered edge cases with reasoning
- Test execution time (if slow, note why)

### Quality Checklist
- [ ] Descriptive test names
- [ ] Clear docstrings
- [ ] Arrange-Act-Assert pattern
- [ ] Appropriate assertions
- [ ] Edge cases covered
- [ ] Error cases covered
- [ ] No test interdependencies
- [ ] Fast execution
- [ ] All tests pass
