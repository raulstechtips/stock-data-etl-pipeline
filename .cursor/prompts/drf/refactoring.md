---
name: "Refactoring Template"
category: "refactor"
description: "Systematic refactoring for Python/Django REST Framework applications"
version: "1.0"
variables:
  - name: "target_code"
    description: "Code, file, or module to refactor"
    required: true
  - name: "refactoring_goal"
    description: "What to improve: 'readability', 'performance', 'modularity', 'maintainability'"
    required: true
  - name: "constraints"
    description: "What must remain unchanged (API contracts, behavior, etc.)"
    required: false
---

# Refactoring Template

## Context

This template guides systematic refactoring in Python/Django REST Framework applications. Refactoring improves code structure while preserving behavior. It emphasizes small, safe changes with tests validating each step.

## Instructions

### Step 1: Understand Current State
- Review target code: `{{target_code}}`
- What does the code currently do?
- What are its dependencies?
- What tests exist for this code?
- What is the public API/contract?
- What are the pain points?

### Step 2: Define Target State
- Refactoring goal: `{{refactoring_goal}}`
- What specific improvements to make?
- What patterns should be applied?
- What Django REST Framework conventions should be followed?
- What are the constraints: `{{constraints}}`

### Step 3: Plan Refactoring Steps
- Break down into smallest safe changes
- Order changes to maintain working code throughout
- Identify which tests need updating vs staying same
- Plan for each change to be independently testable
- Consider creating new code alongside old before switching

### Step 4: Execute Refactoring
- Make one logical change at a time
- Run tests after each change
- Commit working states frequently
- Use automated refactoring tools where available
- Keep code working at all times

### Step 5: Validate
- All existing tests pass
- Behavior unchanged (unless intentionally modified)
- Code meets style guidelines for Python
- Performance not regressed (verify if performance-critical)
- API contract preserved (if applicable)

## Constraints

- **Preserve Behavior**: Unless explicitly changing it
- **Maintain Tests**: Tests should pass throughout (update only when necessary)
- **Small Steps**: Each change should be independently reviewable
- **Keep It Working**: Code should remain functional after each change
- **Follow Conventions**: Apply Django REST Framework/Python best practices

## Common Refactoring Patterns

### For Readability
- Extract method/function
- Rename variables/functions for clarity
- Break up large functions
- Remove code duplication
- Simplify conditional logic
- Add meaningful comments

### For Performance
- Optimize database queries using Django
- Add caching where appropriate
- Reduce unnecessary computations
- Use more efficient algorithms
- Batch operations
- Lazy load data

### For Modularity
- Extract class/module
- Move related functions together
- Reduce coupling between modules
- Apply dependency injection
- Separate concerns

### For Maintainability
- Reduce complexity
- Remove dead code
- Update dependencies
- Standardize patterns
- Improve error handling

## Refactoring Checklist

### Planning
- [ ] Current behavior documented
- [ ] Tests exist for current behavior (APITestCase/pytest)
- [ ] Refactoring goals clearly defined
- [ ] Constraints identified
- [ ] Steps planned
- [ ] Database queries analyzed (use Django Debug Toolbar)

### Execution
- [ ] Changes made incrementally
- [ ] Tests run after each change (`python manage.py test` or `pytest`)
- [ ] Code committed frequently
- [ ] Each commit has working code
- [ ] Migrations created if models changed

### Validation
- [ ] All tests pass
- [ ] Behavior preserved (or intentionally changed)
- [ ] Performance not regressed (check query count)
- [ ] Code follows Python/DRF conventions
- [ ] Type hints added where appropriate
- [ ] Docstrings updated
- [ ] API documentation updated if endpoints changed

## Expected Output

### 1. Refactored Code
- Improved code meeting the refactoring goal
- All changes applied systematically
- Working, tested code
- Follows Python style guide (PEP 8, Google Python Style Guide)

### 2. Test Updates (if any)
- Updated tests reflecting structural changes
- New tests if refactoring exposed gaps
- All tests passing
- Tests use APITestCase or pytest-django

### 3. Summary
- What was refactored and why
- Key improvements made
- Any trade-offs or considerations
- Performance impact (query count before/after)
- Before/after metrics:
  - Lines of code
  - Cyclomatic complexity
  - Number of database queries
  - Response time (if measured)

### 4. Migration Notes (if API changed)
- How to update calling code (frontend, mobile apps)
- Deprecation warnings if phasing out old endpoints
- Timeline for complete migration
- Backwards compatibility strategy
