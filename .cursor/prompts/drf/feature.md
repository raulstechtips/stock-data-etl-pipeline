---
name: "Feature Implementation Template"
category: "feature"
description: "Structured approach to implementing new features in Django/DRF applications following service layer patterns"
version: "1.0"
variables:
  - name: "feature_name"
    description: "Name of the feature to implement (e.g., 'Task Tagging System', 'User Notifications')"
    required: true
  - name: "requirements"
    description: "Detailed feature requirements and specifications"
    required: true
  - name: "acceptance_criteria"
    description: "How to verify the feature works correctly"
    required: true
  - name: "affected_modules"
    description: "Which parts of the system will be modified (models, API, services, etc.)"
    required: false
---

# Feature Implementation Template

## Context

This template guides full-stack feature implementation in Django/DRF applications, following established architectural patterns including service layer, proper serialization, authentication/authorization, and comprehensive testing.

### Django/DRF Architecture Flow
1. **Models**: Database schema and business objects
2. **Services**: Business logic layer (separate from views)
3. **Serializers**: Data validation and transformation
4. **Views/ViewSets**: HTTP request handling
5. **URLs**: API endpoint routing
6. **Tests**: Comprehensive coverage

## Instructions

### Step 1: Planning & Design
- Analyze requirements: `{{feature_name}}`
- Review specifications: `{{requirements}}`
- Identify affected modules: `{{affected_modules}}`
- Design data models and relationships
- Plan API endpoints and HTTP methods
- Consider security and permissions
- Identify external dependencies

### Step 2: Implement Models
- Create or modify Django models
- Define fields with appropriate types and constraints
- Add model methods for business logic
- Create custom managers if needed
- Add `__str__` and `__repr__` methods
- Include docstrings explaining the model purpose

### Step 3: Implement Service Layer
- Create service class in `services/` directory
- Encapsulate business logic (validation, calculations, workflows)
- Handle transactions using `@transaction.atomic`
- Add logging at key decision points
- Keep services independent of HTTP/request context
- Return domain objects, not serialized data

### Step 4: Implement Serializers
- Create model serializers for data validation
- Add custom validation methods
- Handle nested relationships
- Define read-only and write-only fields
- Add serializer-level validation logic

### Step 5: Implement Views/ViewSets
- Create ViewSet or APIView classes
- Configure authentication and permission classes
- Call service layer for business logic
- Keep views thin (delegate to services)
- Handle errors gracefully with proper status codes
- Add OpenAPI documentation strings

### Step 6: Configure URLs
- Add URL patterns for new endpoints
- Use routers for ViewSets
- Follow REST conventions (plural nouns, proper HTTP methods)

### Step 7: Write Tests
- Model tests (validation, methods, managers)
- Service tests (business logic, transactions)
- API tests (endpoints, permissions, status codes)
- Edge cases and error scenarios
- Achieve meaningful coverage

### Step 8: Documentation
- Add docstrings to all classes and methods
- Document API endpoints
- Update relevant README sections if needed

## Constraints

- **Service Layer Pattern**: Business logic goes in service classes, not views
- **DRF Conventions**: Follow REST best practices and DRF patterns
- **Security First**: Implement proper authentication, permissions, and input validation
- **Transaction Safety**: Use `@transaction.atomic` for multi-step operations
- **QuerySet Optimization**: Use `select_related` and `prefetch_related` appropriately
- **Logging**: Add structured logging at INFO level for important actions
- **Type Hints**: Use Python type hints for better code clarity
- **Testing**: Comprehensive test coverage for all new code


## Expected Output

### 1. Database Layer
- Model definitions with proper fields, relationships, and constraints
- Migrations (generated via `makemigrations`)

### 2. Business Logic Layer
- Service class(es) with well-documented methods
- Transaction handling for data integrity
- Appropriate logging

### 3. API Layer
- Serializers for validation and data transformation
- ViewSet/APIView with proper HTTP methods
- URL configuration

### 4. Tests
- Model tests (at least 3-5 test cases)
- Service tests (business logic coverage)
- API tests (endpoint functionality and permissions)
- Edge case coverage

### 5. Documentation
- Docstrings on all new classes and methods
- Brief summary of implementation approach
- Any known limitations or future improvements
