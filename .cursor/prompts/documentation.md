---
name: "Documentation Generation Template"
category: "docs"
description: "Generate high-quality documentation for code, APIs, and project components"
version: "1.0"
variables:
  - name: "target"
    description: "What to document (e.g., 'TaskService class', 'Tasks API endpoints', 'Installation guide')"
    required: true
  - name: "doc_type"
    description: "Type of documentation: 'code' (docstrings), 'api' (endpoint docs), 'guide' (user/dev guide), 'readme' (project overview)"
    required: true
  - name: "audience"
    description: "Target audience: 'developers', 'end-users', 'api-consumers', 'maintainers'"
    required: true
  - name: "include_examples"
    description: "Whether to include code examples (true/false)"
    required: false
---

# Documentation Generation Template

## Context

This template guides creation of clear, accurate, and useful documentation across different contexts: code docstrings, API documentation, user guides, and README sections. Documentation should be concise yet comprehensive, with practical examples where appropriate.

### Documentation Types
- **Code Documentation**: Docstrings for classes, methods, and functions
- **API Documentation**: REST endpoint specifications with examples
- **User Guides**: How-to guides for specific tasks
- **Project Documentation**: README, setup instructions, architecture overview

## Instructions

### Step 1: Analyze the Target
- Read and understand: `{{target}}`
- Identify documentation type: `{{doc_type}}`
- Consider audience: `{{audience}}`
- Determine scope and depth needed

### Step 2: Extract Key Information
For **code documentation**:
- Purpose and responsibility
- Parameters and types
- Return values
- Exceptions raised
- Side effects

For **API documentation**:
- Endpoint URL and HTTP method
- Authentication requirements
- Request parameters (path, query, body)
- Response format and status codes
- Error responses

For **guides**:
- Prerequisites
- Step-by-step instructions
- Expected outcomes
- Common issues and solutions

### Step 3: Structure the Documentation
- Start with a clear summary/overview
- Organize logically (general to specific)
- Use appropriate formatting (headers, lists, code blocks)
- Include examples if `{{include_examples}}` is true
- Add related links or cross-references

### Step 4: Review for Quality
- **Accuracy**: Information is correct and up-to-date
- **Completeness**: All necessary information included
- **Clarity**: Easy to understand for target audience
- **Conciseness**: No unnecessary verbosity
- **Examples**: Practical, realistic use cases

## Constraints

- **Concise**: Be thorough but avoid unnecessary details
- **Accurate**: Ensure all information is correct
- **Practical**: Include real-world examples when helpful
- **Formatted**: Use proper markdown/formatting
- **Consistent**: Follow existing documentation style
- **Current**: Reference current code/API state


## Expected Output

### For Code Documentation (`doc_type: code`)
- Complete docstring following Google or NumPy style
- Clear parameter descriptions with types
- Return value documentation
- Exception documentation
- Usage example if applicable

### For API Documentation (`doc_type: api`)
- Endpoint URL and HTTP method
- Authentication requirements
- Complete parameter documentation
- Response examples with status codes
- Error response documentation
- cURL or code examples

### For User Guides (`doc_type: guide`)
- Clear step-by-step instructions
- Prerequisites listed upfront
- Practical examples
- Common issues and solutions
- Links to related documentation

### For README Sections (`doc_type: readme`)
- Project overview and purpose
- Quick start guide
- Installation instructions
- Key features highlight
- Links to detailed documentation
- Contributing guidelines (if applicable)

## Quality Checklist

- [ ] Information is accurate and matches current implementation
- [ ] Language is clear and appropriate for target audience
- [ ] Examples are practical and realistic
- [ ] All parameters/fields are documented
- [ ] Error cases are covered
- [ ] Formatting is consistent and readable
- [ ] Cross-references are included where helpful
