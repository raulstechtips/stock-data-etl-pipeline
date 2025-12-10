# Cursor Prompt Templates

This directory contains reusable prompt templates for common development workflows. Templates provide structured guidance for AI assistants to help with consistent, high-quality code generation.

## Template Collections

### DRF/API Templates (This Directory)
Templates for Django REST Framework applications: API endpoints, serializers, views, and service layer patterns.
- `bug-fix.md` - Debug and fix DRF/API issues
- `feature.md` - Implement REST API features
- `documentation.md` - Document APIs and code
- `test-writing.md` - Test API endpoints and services

### Generic Microservice Templates (`/generic/`)
Templates for Django-integrated microservices that run in separate containers:
- Background workers (Celery/RQ)
- CLI tools (management commands)
- Scheduled jobs (cron, periodic tasks)
- PySpark data transformation jobs

**See**: [Generic Templates README](./generic/README.md) for details.

## Quick Start

### Using a Template

Reference a template in your chat with the `@` symbol:

```
@.cursor/prompts/bug-fix.mdc

component_name: "TaskSerializer"
bug_description: "Tasks with null due_date cause serialization error"
expected_behavior: "Should handle null due_date gracefully"
actual_behavior: "Raises TypeError when serializing"
```

The AI assistant will follow the template's structure and examples to help solve your problem.

## Template Catalog

### 🐛 Bug Fix Template
**File**: `bug-fix.mdc`  
**Category**: Debugging  
**Purpose**: Systematic approach to identifying, fixing, and validating bugs in Django/DRF applications

**Variables**:
- `component_name` (required): Component where the bug occurs
- `bug_description` (required): Clear description of the bug
- `expected_behavior` (required): What should happen
- `actual_behavior` (required): What actually happens
- `error_message` (optional): Error messages or stack traces

**Use Cases**:
- Model method bugs
- Serializer validation issues
- View/API endpoint errors
- N+1 query problems
- Data validation failures

---

### ✨ Feature Implementation Template
**File**: `feature.mdc`  
**Category**: Development  
**Purpose**: Structured approach to implementing new features following Django/DRF best practices

**Variables**:
- `feature_name` (required): Name of the feature
- `requirements` (required): Detailed feature requirements
- `acceptance_criteria` (required): How to verify it works
- `affected_modules` (optional): Which parts of system will be modified

**Use Cases**:
- New API endpoints
- Adding models and relationships
- Implementing business logic in service layer
- Full-stack feature development

**Example**:
```
@.cursor/prompts/feature.mdc

feature_name: "Task Tagging System"
requirements: "Users should be able to add multiple tags to tasks. Tags can be reused across tasks."
acceptance_criteria: "Users can create tasks with tags, view tasks filtered by tag, and manage tags"
affected_modules: "models, serializers, API endpoints"
```

---

### 📚 Documentation Template
**File**: `documentation.mdc`  
**Category**: Documentation  
**Purpose**: Generate high-quality documentation for code, APIs, and project components

**Variables**:
- `target` (required): What to document
- `doc_type` (required): Type of documentation (`code`, `api`, `guide`, `readme`)
- `audience` (required): Target audience (`developers`, `end-users`, `api-consumers`, `maintainers`)
- `include_examples` (optional): Whether to include code examples

**Use Cases**:
- Writing docstrings for classes and methods
- Creating API endpoint documentation
- Generating user guides
- Writing README sections

**Example**:
```
@.cursor/prompts/documentation.mdc

target: "TaskService.complete_task method"
doc_type: "code"
audience: "developers"
include_examples: true
```

---

### 🧪 Test Writing Template
**File**: `test-writing.mdc`  
**Category**: Testing  
**Purpose**: Write comprehensive tests for Django/DRF applications with proper coverage

**Variables**:
- `component_name` (required): Component to test
- `test_type` (required): Type of test (`model`, `service`, `api`, `integration`, `unit`)
- `coverage_areas` (optional): Specific areas to cover

**Use Cases**:
- Model tests (validation, methods, managers)
- Service layer tests (business logic)
- API endpoint tests (authentication, permissions, responses)
- Integration tests (end-to-end workflows)

**Example**:
```
@.cursor/prompts/test-writing.mdc

component_name: "TaskService"
test_type: "service"
coverage_areas: "task completion, validation, edge cases, transaction rollback"
```

## Template Structure

All templates follow a consistent structure:

### YAML Front-matter
```yaml
---
name: "Template Name"
category: "bug-fix|feature|docs|testing"
description: "Brief description"
version: "1.0"
variables:
  - name: "variable_name"
    description: "What this variable represents"
    required: true/false
---
```

### Markdown Body

1. **Context**: Background information and approach
2. **Instructions**: Step-by-step guidance for the AI
3. **Constraints**: Requirements and limitations
4. **Examples**: 2-3 concrete before/after examples
5. **Expected Output**: Format and structure of deliverable

## Best Practices

### Writing Good Variable Values

**Be Specific**:
- ❌ "Fix the bug in the serializer"
- ✅ "TaskSerializer raises ValidationError when due_date is None"

**Provide Context**:
- ❌ "Add tags feature"
- ✅ "Add many-to-many tag system allowing tasks to have multiple reusable tags"

**Include Relevant Details**:
```
bug_description: "When user completes a task, the completed_at timestamp is 
saved in UTC but displayed in local time, causing confusion in negative UTC 
offset timezones"
```

### Combining Templates

You can reference multiple templates in sequence:

```
1. Use feature.mdc to implement the feature
2. Use test-writing.mdc to add comprehensive tests
3. Use documentation.mdc to document the new feature
```

### When NOT to Use Templates

Templates are designed for structured, repeatable workflows. Don't use them for:
- Simple questions or clarifications
- Exploratory code analysis
- Quick one-line changes
- General discussion

## Integration with Project Rules

These prompt templates work alongside the project's `.cursor/rules/` files:

- **coding-practices.mdc**: Logging, Django patterns, service layer
- **django-transactions.mdc**: Atomic operations, race conditions
- **drf-error-handling.mdc**: Exception handling in DRF
- **drf-security.mdc**: Authentication, permissions, validation
- **error-handling.mdc**: Python exception best practices
- **python-style.mdc**: Code style, type hints, SOLID principles
- **testing-patterns.mdc**: Testing best practices

The AI assistant automatically references relevant rules when using templates. You don't need to explicitly mention them.

## Creating Custom Templates

To add a new template:

1. **Create a new `.mdc` file** in this directory
2. **Follow the standard structure**:
   - YAML front-matter with name, category, description, version, variables
   - Markdown sections: Context, Instructions, Constraints, Examples, Expected Output
3. **Include 2-3 concrete examples** showing before/after or complete implementations
4. **Update this README** to add your template to the catalog

### Template Naming Convention
- Use lowercase with hyphens: `my-template.mdc`
- Make names descriptive: `api-endpoint.mdc` not `endpoint.mdc`
- Keep names concise: 2-3 words maximum

## Tips for Effective Use

1. **Read the template first**: Understand what guidance it provides
2. **Fill all required variables**: Missing information leads to generic output
3. **Be detailed in descriptions**: More context = better results
4. **Review the examples**: They show the expected level of detail
5. **Iterate**: Refine your variable values if the output isn't quite right

## Examples Gallery

### Example 1: Fixing a Serializer Bug

```
@.cursor/prompts/bug-fix.mdc

component_name: "TaskSerializer"
bug_description: "When creating a task with tags, the tags field accepts tag IDs 
but doesn't validate they exist, causing 500 errors"
expected_behavior: "Should validate tag IDs exist and return 400 with clear error 
if any are invalid"
actual_behavior: "Returns 500 Internal Server Error when invalid tag ID provided"
error_message: "Task matching query does not exist"
```

### Example 2: Implementing a New Feature

```
@.cursor/prompts/feature.mdc

feature_name: "Task Priority System"
requirements: "Add priority field to tasks (low, medium, high, critical). Users 
should be able to filter tasks by priority. Default priority is medium."
acceptance_criteria: "Tasks can be created with priority. API endpoint supports 
filtering by priority. Priority is displayed in task list."
affected_modules: "Task model, TaskSerializer, TaskViewSet, migrations"
```

### Example 3: Writing API Documentation

```
@.cursor/prompts/documentation.mdc

target: "Tasks API endpoints (list, create, retrieve, update, delete, complete)"
doc_type: "api"
audience: "api-consumers"
include_examples: true
```

### Example 4: Writing Comprehensive Tests

```
@.cursor/prompts/test-writing.mdc

component_name: "TaskViewSet"
test_type: "api"
coverage_areas: "authentication, permissions, CRUD operations, task completion 
endpoint, filtering, pagination, error responses"
```

## Feedback and Improvements

These templates are living documents. If you find:
- Missing edge cases in examples
- Unclear instructions
- Opportunities for new templates
- Ways to improve existing templates

Feel free to update them! The goal is to continuously improve the quality and usefulness of these templates.

## Related Resources

### Generic Microservice Templates
For non-API components like background workers, CLI tools, scheduled jobs, and PySpark jobs, see the [Generic Templates](./generic/README.md) directory.

### Project Rules
These templates work alongside project rules in `.cursor/rules/`:
- `coding-practices.mdc` - Django patterns, logging
- `django-transactions.mdc` - Atomic operations
- `drf-error-handling.mdc` - DRF exception handling
- `drf-security.mdc` - API security
- `python-style.mdc` - Code style guidelines
- `testing-patterns.mdc` - Test best practices

