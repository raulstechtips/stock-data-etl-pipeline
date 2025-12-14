---
name: "Setup Assistant"
category: "meta"
description: "Use this prompt to customize a generic prompt or rule template for your specific tech stack"
version: "1.0"
variables:
  - name: "target_file"
    description: "Which template to customize (e.g., 'prompts/bug-fix.md', 'rules/coding-practices.md')"
    required: true
  - name: "language"
    description: "Primary programming language (e.g., TypeScript, Python, Go, Java)"
    required: true
  - name: "framework"
    description: "Main framework/library (e.g., Express, Django, Gin, Spring Boot)"
    required: false
  - name: "database"
    description: "Primary database (e.g., PostgreSQL, MongoDB, MySQL)"
    required: false
  - name: "orm"
    description: "ORM/data layer (e.g., Prisma, Django ORM, GORM, Hibernate)"
    required: false
  - name: "test_framework"
    description: "Testing framework (e.g., Jest, pytest, testing, JUnit)"
    required: false
  - name: "package_manager"
    description: "Package manager (e.g., npm, pip, go mod, maven)"
    required: false
  - name: "api_style"
    description: "API architecture (e.g., REST, GraphQL, gRPC)"
    required: false
  - name: "external_references"
    description: "URLs to official docs or guides for additional context"
    required: false
  - name: "additional_context"
    description: "Team conventions, patterns, or specific requirements"
    required: false
---

# Setup Assistant

## Context

This template guides customization of generic prompt and rule templates for your specific tech stack. It replaces all `{{VARIABLE}}` placeholders with your technologies and fills in `[PLACEHOLDER]` sections with framework-specific patterns, common issues, and realistic code examples if required.

### Customization Approach
1. Read the target generic template file
2. Replace all variables with specific technologies
3. Fill placeholders with framework-specific patterns
4. Add realistic code examples if required for the tech stack
5. Include common issues/patterns specific to the stack

## Instructions

### Step 1: Identify Template to Customize
- Target file: `{{target_file}}`
- Read the generic template to understand its structure
- Note all `{{VARIABLE}}` placeholders and `[PLACEHOLDER]` sections

### Step 2: Apply Technology Stack
Replace variables with specific values:
- `{{LANGUAGE}}` → `{{language}}`
- `{{FRAMEWORK}}` → `{{framework}}`
- `{{DATABASE}}` → `{{database}}`
- `{{ORM}}` → `{{orm}}`
- `{{TEST_FRAMEWORK}}` → `{{test_framework}}`
- `{{PACKAGE_MANAGER}}` → `{{package_manager}}`
- `{{API_STYLE}}` → `{{api_style}}`

### Step 3: Fill Framework-Specific Content

**For Prompt Templates (bug-fix, feature, test-writing, etc.):**
- Add common issues specific to `{{framework}}`
- Add framework-specific debugging steps
- Include typical error patterns for the stack
- Fill placeholder sections with framework-specific patterns

**For Rule Templates (coding-practices, error-handling, etc.):**
- Use `{{external_references}}` if provided, otherwise rely on your knowledge of `{{language}}` standards
- Add language-specific style conventions
- Include framework-specific patterns and best practices
- Add ORM-specific query optimization patterns
- Include testing patterns for `{{test_framework}}`

### Step 4: Use External References
If `{{external_references}}` provided:
- Review the documentation
- Extract relevant patterns and conventions
- Incorporate framework-specific best practices into instructions

### Step 5: Incorporate Additional Context
If `{{additional_context}}` provided:
- Add team-specific conventions
- Include project-specific patterns
- Adjust examples if required to match team style
- Add any custom constraints or requirements

### Step 6: Review and Validate
- Ensure all `{{VARIABLE}}` placeholders are replaced
- Verify all `[PLACEHOLDER]` sections are filled
- Check that code examples are realistic and correct if required
- Confirm examples follow `{{language}}`/`{{framework}}` conventions if required

## Constraints

- **Preserve Structure**: Keep the original template's section structure intact
- **Framework Accuracy**: Use correct patterns and idioms for `{{framework}}`
- **Language Conventions**: Follow `{{language}}` style guide and conventions
- **Realistic Examples**: Code examples must be production-ready, not pseudo-code if required
- **Completeness**: Fill all placeholders; don't leave any `{{VARIABLE}}` or `[PLACEHOLDER]`
- **Consistency**: Use consistent naming and patterns throughout

## Expected Output

### Customized Template File

Provide the complete customized file contents with:

**1. YAML Front-matter**
- All metadata preserved
- Variables section intact (for end users)

**2. Replaced Variables**
- All `{{VARIABLE}}` replaced with specific technologies
- Technology names capitalized correctly

**3. Filled Placeholders**
- All `[PLACEHOLDER]` sections filled with specific patterns
- Framework-specific issues and solutions
- Code examples only if template explicitly requests them

### Summary of Changes

Brief list of:
- Key patterns added
- Common issues included
- Examples provided
- Any stack-specific considerations
