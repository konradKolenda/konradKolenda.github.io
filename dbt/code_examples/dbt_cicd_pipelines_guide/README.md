# dbt CI/CD Pipelines Code Examples

This directory contains comprehensive code examples for implementing CI/CD pipelines with dbt, from basic automation to advanced deployment patterns.

## Files Overview

### GitHub Actions Workflows

- **`basic_github_workflow.yml`** - Simple CI/CD pipeline for getting started
- **`advanced_multi_environment.yml`** - Production-ready multi-environment pipeline with linting and matrix testing
- **`blue_green_deployment.yml`** - Blue-green deployment pattern for zero-downtime releases
- **`dynamic_environments.yml`** - Creates temporary environments for each pull request

### Configuration Files

- **`profiles.yml`** - Multi-environment dbt profiles configuration
- **`test_email_format.sql`** - Custom generic test example for data validation
- **`monitoring_macros.sql`** - Macros for deployment monitoring, performance tracking, and alerting

## Quick Start

1. **Basic Setup**: Start with `basic_github_workflow.yml` to set up fundamental CI/CD
2. **Add Testing**: Incorporate `test_email_format.sql` for custom data quality tests
3. **Multi-Environment**: Upgrade to `advanced_multi_environment.yml` for staging/production separation
4. **Advanced Patterns**: Implement `blue_green_deployment.yml` or `dynamic_environments.yml` for sophisticated deployment strategies

## Required GitHub Secrets

```
DBT_SNOWFLAKE_ACCOUNT
DBT_SNOWFLAKE_USER
DBT_SNOWFLAKE_PASSWORD
DBT_SNOWFLAKE_ROLE
DBT_SNOWFLAKE_DATABASE
DBT_SNOWFLAKE_WAREHOUSE
DBT_SNOWFLAKE_SCHEMA
```

## Environment Variables

Common environment variables used across workflows:

```bash
DBT_PROFILES_DIR=./
DBT_PROJECT_DIR=./
GITHUB_RUN_ID # Automatic
GITHUB_ACTOR # Automatic  
GITHUB_SHA # Automatic
GITHUB_REF # Automatic
```

## Usage Examples

### Basic CI/CD Pipeline
Copy `basic_github_workflow.yml` to `.github/workflows/dbt_ci.yml` in your repository.

### Custom Tests
Add `test_email_format.sql` to your `macros/` directory and reference in schema.yml:

```yaml
models:
  - name: customers
    columns:
      - name: email
        tests:
          - email_format
```

### Monitoring Setup
Add monitoring macros to your dbt project and configure hooks in `dbt_project.yml`:

```yaml
on-run-end:
  - "{{ log_deployment_audit() }}"
  - "{{ log_model_performance() }}"
  - "{{ alert_on_test_failure() }}"
```

## Best Practices

1. **Start Simple**: Begin with basic workflow and add complexity gradually
2. **Environment Separation**: Always use separate schemas/databases for different environments
3. **Caching**: Use GitHub Actions caching for dbt packages to speed up builds
4. **Security**: Store all credentials in GitHub Secrets, never in code
5. **Monitoring**: Implement comprehensive logging and alerting from the start
6. **Testing**: Run full test suite in CI, including custom data quality tests

## Advanced Features

- **Blue-Green Deployments**: Zero-downtime production deployments
- **Dynamic Environments**: Automatic PR environment creation and cleanup
- **Performance Monitoring**: Track model execution times and resource usage
- **Custom Alerting**: Slack/Teams notifications for failures and deployments
- **State-Based Deployments**: Deploy only changed models using dbt state

## Troubleshooting

Common issues and solutions:

1. **Memory Issues**: Use larger GitHub Actions runners or optimize thread counts
2. **Timeout Problems**: Split long-running jobs or increase timeout values
3. **Permission Errors**: Verify database roles and permissions for each environment
4. **Caching Issues**: Clear GitHub Actions cache if packages are stale

For detailed implementation guidance, see the main documentation at [dbt CI/CD Pipelines Guide](../dbt_cicd_pipelines_guide.html).