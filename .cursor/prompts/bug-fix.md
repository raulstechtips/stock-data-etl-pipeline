---
name: "Bug Fix Template"
category: "bug-fix"
description: "Systematic approach to identifying, fixing, and validating bugs in Django/DRF applications"
version: "1.0"
variables:
  - name: "component_name"
    description: "Name of the component/file where the bug occurs (e.g., UserSerializer, OrderService, TaskViewSet)"
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
    description: "Any error messages or stack traces (if applicable)"
    required: false
---

# Bug Fix Template

## Context

This template guides systematic bug fixing in Django/DRF applications. It emphasizes root cause analysis over symptomatic fixes, minimal code changes, and proper validation.

### Django/DRF Debugging Approach
1. Reproduce the issue reliably
2. Identify the root cause (not just symptoms)
3. Implement minimal, focused fix
4. Add appropriate logging
5. Validate the fix with tests
6. Ensure no regressions

## Instructions

### Step 1: Analyze the Bug
- Read the component code: `{{component_name}}`
- Understand the bug: `{{bug_description}}`
- Compare expected vs actual: `{{expected_behavior}}` vs `{{actual_behavior}}`
- Review error messages: `{{error_message}}`
- Identify related code (models, serializers, services, views)

### Step 2: Root Cause Analysis
- Trace the code flow from entry point to bug location
- Check for common Django/DRF issues:
  - N+1 queries (missing select_related/prefetch_related)
  - Serializer validation errors
  - Missing transaction boundaries
  - Incorrect queryset filtering
  - Permission/authentication issues
  - Timezone-related bugs
- Identify the minimal change needed

### Step 3: Implement Fix
- Make the smallest code change that fixes the root cause
- Follow existing code patterns and conventions
- Add logging at appropriate level (DEBUG/INFO/WARNING/ERROR)
- Update docstrings if behavior changes
- Consider edge cases

### Step 4: Validation
- Write or update tests to cover the bug scenario
- Run existing tests to ensure no regressions
- Manually test the fix if applicable
- Check for similar bugs in related code

## Constraints

- **Minimal Changes**: Only modify what's necessary to fix the root cause
- **Pattern Consistency**: Follow existing project patterns and conventions
- **No Breaking Changes**: Ensure backward compatibility unless explicitly required
- **Add Logging**: Include appropriate log statements for debugging
- **Test Coverage**: Add tests that would have caught this bug
- **Documentation**: Update docstrings/comments if behavior changes

## Expected Output

### 1. Fixed Code
- Modified file(s) with bug fix applied
- Minimal, focused changes
- Added logging statements where appropriate

### 2. Test Coverage
- New test case(s) that verify the fix
- Tests that would have caught this bug originally

### 3. Summary
- Brief explanation of root cause
- Changes made and why
- Any side effects or considerations
- Confirmation that existing tests still pass
