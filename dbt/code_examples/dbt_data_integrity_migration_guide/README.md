# Data Integrity Migration: Code Examples

This directory contains practical code examples demonstrating how to migrate from traditional SQL Server constraints to dbt's data testing and validation approach.

## File Organization

### 1. `01_traditional_vs_dbt_constraints.sql`
- **Purpose**: Side-by-side comparison of SQL Server table definitions vs dbt model definitions
- **Key Concepts**: PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL, CHECK constraints vs dbt equivalents
- **Use Case**: Understanding the fundamental differences in approach

### 2. `02_schema_yml_tests.yml`
- **Purpose**: Complete schema.yml configuration showing dbt data tests as constraint equivalents
- **Key Concepts**: Generic tests (unique, not_null, relationships, accepted_values), dbt_utils tests
- **Use Case**: Implementing basic data integrity validation in dbt

### 3. `03_custom_data_tests.sql`
- **Purpose**: Custom SQL tests for complex business rules that go beyond simple constraints
- **Key Concepts**: Complex CHECK constraint logic, cross-table validation, business rule enforcement
- **Use Case**: Advanced data integrity patterns that require custom SQL logic

### 4. `04_model_contracts_example.yml`
- **Purpose**: Model contracts for schema enforcement at build time
- **Key Concepts**: Column data types, contract enforcement, constraint definitions in YAML
- **Use Case**: Enforcing table schema definitions similar to traditional DDL

### 5. `05_migration_strategy_example.sql`
- **Purpose**: Complete migration strategy from SQL Server to dbt with phased approach
- **Key Concepts**: Assessment, staging, progressive validation, monitoring
- **Use Case**: Step-by-step migration plan for real-world projects

### 6. `06_dbt_project_configuration.yml`
- **Purpose**: Complete dbt_project.yml setup for data integrity focused projects
- **Key Concepts**: Test configurations, severity levels, environment-specific settings
- **Use Case**: Project-level configuration for comprehensive data validation

## Usage Instructions

### Getting Started
1. Review `01_traditional_vs_dbt_constraints.sql` to understand the conceptual differences
2. Examine `02_schema_yml_tests.yml` for basic test implementations
3. Study `05_migration_strategy_example.sql` for phased migration approach

### Implementation Steps
1. **Assessment Phase**: Use the assessment queries in file 05 to inventory existing constraints
2. **Basic Setup**: Implement schema.yml tests from file 02 for core tables
3. **Advanced Validation**: Add custom tests from file 03 for complex business rules
4. **Schema Enforcement**: Implement model contracts from file 04 for critical tables
5. **Project Configuration**: Use file 06 as a template for dbt_project.yml setup

### Required dbt Packages
```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: calogica/dbt_expectations
    version: 0.10.1
```

## Key Migration Concepts

### Traditional Database Constraints → dbt Equivalents
- **PRIMARY KEY** → `unique` + `not_null` tests
- **FOREIGN KEY** → `relationships` test
- **UNIQUE** → `unique` test
- **NOT NULL** → `not_null` test
- **CHECK** → `accepted_values` or custom SQL tests

### Benefits of dbt Approach
- ✅ Version-controlled data validation
- ✅ Flexible business rule implementation
- ✅ Integration with CI/CD pipelines
- ✅ Comprehensive data quality reporting
- ✅ Documentation and lineage tracking

### Trade-offs
- ❌ No immediate data rejection (runtime vs insert-time validation)
- ❌ Requires compute resources for test execution
- ❌ Different mental model from traditional constraints

## Best Practices

1. **Start Small**: Begin with unique and not_null tests for primary keys
2. **Progressive Rollout**: Use warning severity initially, then escalate to error
3. **Store Failures**: Enable `store_failures: true` for data quality analysis
4. **Monitor Performance**: Large table tests can be resource-intensive
5. **Document Business Rules**: Use descriptions to explain test logic
6. **Environment Differences**: Use different severity levels for dev vs prod

## Testing Your Migration

```bash
# Run all data tests
dbt test

# Run tests for specific model
dbt test --models stg_customers

# Run only unique and not_null tests
dbt test --select test_type:generic

# Store test failures for analysis
dbt test --store-failures
```

## Troubleshooting Common Issues

### Performance Issues
- Use `limit` configuration for large table tests
- Consider sampling strategies for data quality monitoring
- Optimize test queries with appropriate indexes

### Test Failures
- Review stored failures in `test_failures_*` tables
- Use `dbt test --store-failures` to capture failure details
- Implement data quality monitoring dashboards

### Schema Evolution
- Model contracts will catch schema changes early
- Use `--warn-error` flag to treat warnings as errors
- Implement version control for schema changes

This collection provides a comprehensive guide for migrating from traditional database constraints to dbt's modern data validation approach while maintaining data integrity standards.