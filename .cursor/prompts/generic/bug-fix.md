---
name: "Generic Bug Fix Template"
category: "bug-fix"
description: "Systematic approach to identifying, fixing, and validating bugs in Django-integrated microservices (background workers, CLI tools, scheduled jobs, PySpark jobs)"
version: "1.0"
variables:
  - name: "component_type"
    description: "Type of component: 'background-worker' (Celery/RQ), 'cli-tool', 'scheduled-job', 'pyspark-job'"
    required: true
  - name: "component_name"
    description: "Name of the component/file where the bug occurs (e.g., SendEmailTask, ImportUsersCommand, DailyReportJob)"
    required: true
  - name: "bug_description"
    description: "Clear description of the bug behavior"
    required: true
  - name: "expected_behavior"
    description: "What should happen"
    required: true
  - name: "actual_behavior"
    description: "What actually happens"
    required: true
  - name: "error_message"
    description: "Any error messages, stack traces, or logs (if applicable)"
    required: false
---

# Generic Bug Fix Template

## Context

This template guides systematic bug fixing in Django-integrated microservices including background workers, CLI tools, scheduled jobs, and PySpark data transformation jobs. It emphasizes root cause analysis over symptomatic fixes, minimal code changes, and proper validation.

### Component Types
- **Background Workers**: Celery tasks, RQ workers, async job processors
- **CLI Tools**: Django management commands, standalone scripts
- **Scheduled Jobs**: Cron jobs, periodic tasks, schedulers
- **PySpark Jobs**: Data transformation, ETL pipelines, batch processing

### Debugging Approach
1. Reproduce the issue reliably
2. Identify the root cause (not just symptoms)
3. Implement minimal, focused fix
4. Add appropriate logging
5. Validate the fix with tests
6. Ensure no regressions

## Instructions

### Step 1: Analyze the Bug
- Component type: `{{component_type}}`
- Read the component code: `{{component_name}}`
- Understand the bug: `{{bug_description}}`
- Compare expected vs actual: `{{expected_behavior}}` vs `{{actual_behavior}}`
- Review error messages/logs: `{{error_message}}`
- Identify related code (models, utilities, external dependencies)

### Step 2: Root Cause Analysis

Trace the code flow and check for common issues by component type:

**For Background Workers:**
- Task retry configuration issues
- Database connection pool exhaustion
- Transaction boundaries in async context
- Race conditions with concurrent tasks
- Serialization/deserialization errors
- Memory leaks in long-running tasks

**For CLI Tools:**
- Argument parsing errors
- Database connection handling
- Transaction management in bulk operations
- Missing error handling for edge cases
- Incorrect queryset filtering
- Permission/file access issues

**For Scheduled Jobs:**
- Idempotency issues (job runs multiple times)
- State management between runs
- Lock/mutex problems with concurrent executions
- Timezone handling in scheduling
- Resource cleanup failures

**For PySpark Jobs:**
- Dataframe schema mismatches
- Partition skew causing failures
- Memory pressure from large datasets
- Django ORM session handling in distributed context
- Broadcast variable serialization
- Driver-executor communication issues

Identify the minimal change needed to fix the root cause.

### Step 3: Implement Fix
- Make the smallest code change that fixes the root cause
- Follow existing code patterns and conventions
- Add logging at appropriate level (DEBUG/INFO/WARNING/ERROR)
- Update docstrings if behavior changes
- Consider edge cases and failure modes
- Ensure containerized deployment compatibility

### Step 4: Validation
- Write or update tests to cover the bug scenario
- Run existing tests to ensure no regressions
- Test in containerized environment if applicable
- Verify fix works with Django ORM integration
- Check for similar bugs in related components

## Constraints

- **Minimal Changes**: Only modify what's necessary to fix the root cause
- **Pattern Consistency**: Follow existing project patterns and conventions
- **No Breaking Changes**: Ensure backward compatibility unless explicitly required
- **Add Logging**: Include appropriate log statements for debugging
- **Test Coverage**: Add tests that would have caught this bug
- **Container Compatibility**: Ensure fix works in containerized environment
- **Django ORM Safety**: Respect connection handling, transactions, and thread safety


## Expected Output

### 1. Fixed Code
- Modified file(s) with bug fix applied
- Minimal, focused changes
- Added logging statements where appropriate
- Container-compatible implementation

### 2. Test Coverage
- New test case(s) that verify the fix
- Tests that would have caught this bug originally
- Tests run successfully in container environment

### 3. Summary
- Brief explanation of root cause
- Changes made and why
- Any side effects or considerations
- Component-specific deployment notes (if applicable)
- Confirmation that existing tests still pass

