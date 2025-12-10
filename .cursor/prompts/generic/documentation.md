---
name: "Generic Documentation Template"
category: "docs"
description: "Generate documentation for Django-integrated microservices (background workers, CLI tools, scheduled jobs, PySpark jobs)"
version: "1.0"
variables:
  - name: "component_type"
    description: "Type of component: 'background-worker' (Celery/RQ), 'cli-tool', 'scheduled-job', 'pyspark-job'"
    required: true
  - name: "target"
    description: "What to document (e.g., 'SendEmailTask', 'ExportReportCommand', 'EventAggregationJob')"
    required: true
  - name: "doc_type"
    description: "Type of documentation: 'code' (docstrings), 'usage' (how-to guide), 'deployment' (ops guide)"
    required: true
  - name: "include_examples"
    description: "Whether to include usage examples (true/false)"
    required: false
---

# Generic Documentation Template

## Context

This template guides creation of clear, accurate documentation for Django-integrated microservices including background workers, CLI tools, scheduled jobs, and PySpark data transformation jobs. Documentation should cover functionality, deployment, monitoring, and troubleshooting.

### Component Types
- **Background Workers**: Celery tasks, RQ workers, async job processors
- **CLI Tools**: Django management commands, standalone scripts
- **Scheduled Jobs**: Cron jobs, periodic tasks, schedulers
- **PySpark Jobs**: Data transformation, ETL pipelines, batch processing

### Documentation Types
- **Code Documentation**: Docstrings, inline comments, module headers
- **Usage Documentation**: How-to guides, command reference, parameters
- **Deployment Documentation**: Setup, configuration, containerization, monitoring

## Instructions

### Step 1: Analyze the Target
- Component type: `{{component_type}}`
- Identify what to document: `{{target}}`
- Determine documentation type: `{{doc_type}}`
- Read implementation code thoroughly
- Identify key functionality and edge cases

### Step 2: Extract Key Information

**For Code Documentation (`doc_type: code`):**
- Purpose and responsibility
- Parameters and types
- Return values
- Exceptions/errors
- Side effects (database changes, external calls)
- Django ORM usage patterns
- Retry/idempotency behavior

**For Usage Documentation (`doc_type: usage`):**
- Prerequisites and setup
- Command syntax or invocation method
- Parameters and options
- Expected behavior
- Common use cases
- Example invocations
- Troubleshooting tips

**For Deployment Documentation (`doc_type: deployment`):**
- Container configuration
- Environment variables
- Dependencies
- Resource requirements
- Scheduling configuration
- Monitoring and alerting
- Logging setup
- Health checks

### Step 3: Structure by Component Type

**For Background Workers:**
- Task signature and parameters
- Trigger mechanism (signal, API, schedule)
- Retry policy and error handling
- Django ORM transaction handling
- Queue configuration
- Monitoring metrics

**For CLI Tools:**
- Command name and syntax
- Arguments and options
- Usage examples
- Input/output formats
- Performance characteristics
- Error codes and messages

**For Scheduled Jobs:**
- Schedule specification (cron, interval)
- Execution environment
- Locking mechanism
- State management
- Monitoring and alerts
- Failure recovery

**For PySpark Jobs:**
- Job invocation method
- Input data sources
- Output destinations
- Spark configuration
- Django ORM integration approach
- Resource requirements
- Partition strategy

### Step 4: Add Examples
If `{{include_examples}}` is true:
- Provide realistic usage examples
- Show common scenarios
- Include expected output
- Demonstrate error handling
- Show monitoring/debugging commands

### Step 5: Review for Quality
- **Accuracy**: Information matches implementation
- **Completeness**: All necessary information included
- **Clarity**: Easy to understand for target audience
- **Practical**: Includes real-world examples
- **Current**: Reflects current implementation

## Constraints

- **Accurate**: All information must match current implementation
- **Complete**: Cover all parameters, errors, and edge cases
- **Concise**: No unnecessary verbosity
- **Practical**: Include realistic examples
- **Formatted**: Use proper markdown formatting
- **Component-Specific**: Address concerns specific to component type

## Expected Output

### For Code Documentation (`doc_type: code`)
- Complete docstring following Google or NumPy style
- Parameter descriptions with types
- Return value documentation
- Exception documentation
- Side effects and dependencies
- Component-specific concerns (retries, transactions, etc.)
- Usage examples
- Monitoring and troubleshooting notes

### For Usage Documentation (`doc_type: usage`)
- Clear command syntax or invocation method
- Complete parameter reference
- Prerequisites and setup steps
- Realistic usage examples
- Expected output format
- Common errors and solutions
- Performance characteristics

### For Deployment Documentation (`doc_type: deployment`)
- Container configuration (Dockerfile, docker-compose)
- Environment variables and secrets
- Scheduling configuration
- Resource requirements
- Monitoring and alerting setup
- Health checks (if applicable)
- Troubleshooting guide
- Rollback procedures

## Quality Checklist

- [ ] Accurate and matches implementation
- [ ] Complete (all parameters, errors, edge cases covered)
- [ ] Clear and appropriate for target audience
- [ ] Includes practical examples
- [ ] Component-specific concerns addressed
- [ ] Monitoring and troubleshooting covered
- [ ] Formatted consistently
- [ ] Current (reflects latest implementation)

