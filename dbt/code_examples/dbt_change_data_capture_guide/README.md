# dbt Change Data Capture (CDC) Implementation Guide - Code Examples

This directory contains comprehensive code examples demonstrating various Change Data Capture (CDC) patterns and implementations in dbt. These examples support the complete CDC guide documentation and provide production-ready templates for implementing robust CDC solutions.

## 📁 Repository Structure

```
dbt_change_data_capture_guide/
├── 01_timestamp_based_cdc.sql       # Timestamp-based CDC implementations
├── 02_log_based_cdc.sql             # Log-based CDC with transaction logs
├── 03_trigger_based_cdc.sql         # Trigger-based CDC using audit tables
├── 04_snapshot_comparison_cdc.sql   # Snapshot comparison methods
├── 05_scd_integration.sql           # SCD Type 1/2/3 integration patterns
├── 06_real_time_processing.sql      # Real-time and streaming CDC
├── 07_performance_optimization.sql  # Performance optimization techniques
├── 08_monitoring_and_alerting.sql   # Monitoring and alerting systems
└── README.md                        # This documentation
```

## 🚀 Quick Start Guide

### Prerequisites
- dbt Core 1.6+ or dbt Cloud
- Supported data warehouse (Snowflake, BigQuery, Redshift, etc.)
- Source systems with CDC capabilities or audit logging
- Basic understanding of SQL and dbt concepts

### Setup Instructions

1. **Clone the examples to your dbt project:**
   ```bash
   # Copy the SQL files to your dbt models directory
   cp 01_timestamp_based_cdc.sql your_dbt_project/models/staging/
   cp 02_log_based_cdc.sql your_dbt_project/models/staging/
   # ... copy other files as needed
   ```

2. **Configure your sources in `schema.yml`:**
   ```yaml
   sources:
     - name: crm
       tables:
         - name: customers
         - name: products
     - name: cdc_logs
       tables:
         - name: customer_changes
         - name: order_changes
   ```

3. **Install required dbt packages:**
   ```yaml
   # packages.yml
   packages:
     - package: dbt-labs/dbt_utils
       version: 1.1.1
   ```

4. **Set up project variables:**
   ```yaml
   # dbt_project.yml
   vars:
     processing_batch_count: 10
     max_memory_batch_size: 50000
     parallel_shards: 8
   ```

## 📚 Code Examples Overview

### 1. Timestamp-Based CDC (`01_timestamp_based_cdc.sql`)
Implements CDC using modification timestamps from source systems.

**Key Features:**
- Basic timestamp filtering with high-water mark tracking
- Soft delete handling
- Timezone normalization
- Performance optimization with partitioning
- Late-arriving data management

**Best Use Cases:**
- Simple incremental data loads
- Systems with reliable update timestamps
- Batch processing scenarios

### 2. Log-Based CDC (`02_log_based_cdc.sql`)
Processes database transaction logs (WAL, binlog) for comprehensive change tracking.

**Key Features:**
- Transaction log parsing and processing
- Before/after value tracking
- Conflict resolution for concurrent updates
- Current state reconstruction
- Complete audit trail maintenance

**Best Use Cases:**
- Real-time replication
- Complete change history requirements
- Systems requiring eventual consistency

### 3. Trigger-Based CDC (`03_trigger_based_cdc.sql`)
Utilizes database triggers and audit tables to capture detailed change information.

**Key Features:**
- Audit table processing
- Change categorization and classification
- User attribution and session tracking
- Current state reconstruction
- Historical change pattern analysis

**Best Use Cases:**
- Detailed audit requirements
- Compliance and regulatory needs
- User behavior analysis

### 4. Snapshot Comparison CDC (`04_snapshot_comparison_cdc.sql`)
Compares full snapshots to identify changes between time periods.

**Key Features:**
- Full dataset comparison logic
- Content hash-based change detection
- Business rule integration
- Batch processing optimization
- Historical trend analysis

**Best Use Cases:**
- Batch processing environments
- Systems without built-in change tracking
- Periodic data synchronization

### 5. SCD Integration (`05_scd_integration.sql`)
Integrates CDC with Slowly Changing Dimension (SCD) patterns.

**Key Features:**
- SCD Type 1, 2, and 3 implementations
- Mixed SCD strategies based on business rules
- Effective date management
- Historical versioning
- Data quality validation

**Best Use Cases:**
- Data warehousing scenarios
- Dimensional modeling requirements
- Historical data preservation needs

### 6. Real-Time Processing (`06_real_time_processing.sql`)
Implements near real-time CDC processing with streaming architectures.

**Key Features:**
- Micro-batch processing
- Stream processing optimizations
- Real-time aggregations
- Low-latency change propagation
- Error handling and recovery

**Best Use Cases:**
- Real-time analytics
- Event-driven architectures
- Low-latency requirements

### 7. Performance Optimization (`07_performance_optimization.sql`)
Advanced patterns for optimizing CDC performance and resource utilization.

**Key Features:**
- Partition-based processing
- Memory optimization techniques
- Parallel processing patterns
- Index optimization strategies
- Cost optimization approaches

**Best Use Cases:**
- Large-scale data processing
- Resource-constrained environments
- Performance-critical applications

### 8. Monitoring and Alerting (`08_monitoring_and_alerting.sql`)
Comprehensive monitoring, alerting, and observability for CDC systems.

**Key Features:**
- Health monitoring dashboards
- Data quality validation
- SLA monitoring and alerting
- Anomaly detection
- Executive reporting

**Best Use Cases:**
- Production CDC systems
- Operations and monitoring
- Business intelligence reporting

## 🛠 Implementation Patterns

### Basic Implementation Flow

1. **Choose Your CDC Pattern:**
   ```sql
   -- For simple timestamp-based CDC
   {{ config(materialized='incremental', unique_key='id') }}
   
   SELECT * FROM source_table
   {% if is_incremental() %}
     WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
   {% endif %}
   ```

2. **Add Change Detection:**
   ```sql
   -- Add CDC metadata
   SELECT 
     *,
     CURRENT_TIMESTAMP as processed_at,
     'UPSERT' as cdc_operation_type
   FROM source_data
   ```

3. **Implement Deduplication:**
   ```sql
   -- Handle duplicates within batch
   ROW_NUMBER() OVER (
     PARTITION BY unique_key 
     ORDER BY updated_at DESC
   ) = 1
   ```

4. **Add Monitoring:**
   ```sql
   -- Track processing metrics
   COUNT(*) as records_processed,
   MAX(updated_at) as latest_timestamp,
   CURRENT_TIMESTAMP as batch_completed_at
   ```

### Configuration Best Practices

```yaml
# Model configuration template
models:
  your_project:
    staging:
      +materialized: incremental
      +unique_key: id
      +on_schema_change: sync_all_columns
      +partition_by:
        field: updated_date
        data_type: date
        granularity: day
      +cluster_by: [customer_id, status]
```

## 📊 Performance Guidelines

### Small Datasets (< 1M records)
- Use simple timestamp-based CDC
- Single-threaded processing
- Basic deduplication logic

### Medium Datasets (1M - 100M records)
- Implement partitioning strategies
- Use batch processing
- Add performance monitoring

### Large Datasets (> 100M records)
- Enable parallel processing
- Optimize memory usage
- Implement advanced partitioning
- Use streaming patterns where applicable

## 🔍 Testing and Validation

### Data Quality Tests
```sql
-- Test for duplicates
SELECT unique_key, COUNT(*)
FROM {{ ref('your_cdc_model') }}
GROUP BY unique_key
HAVING COUNT(*) > 1

-- Test for missing timestamps
SELECT COUNT(*)
FROM {{ ref('your_cdc_model') }}
WHERE updated_at IS NULL
```

### Performance Tests
```sql
-- Monitor processing rates
SELECT 
  COUNT(*) / EXTRACT(EPOCH FROM (MAX(processed_at) - MIN(processed_at))) as records_per_second
FROM {{ ref('your_cdc_model') }}
WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
```

## 🚨 Troubleshooting Guide

### Common Issues

1. **Missing Changes**
   - Check timezone handling
   - Verify timestamp filtering logic
   - Add buffer time for late arrivals

2. **Duplicate Records**
   - Review deduplication logic
   - Check unique key definitions
   - Implement proper ordering

3. **Performance Issues**
   - Optimize partition strategy
   - Review index usage
   - Consider batch size tuning

4. **Data Quality Problems**
   - Implement validation tests
   - Add monitoring alerts
   - Review source data quality

## 📈 Monitoring Setup

### Key Metrics to Monitor
- Processing lag (time between source update and processing)
- Throughput (records processed per minute)
- Success rate (% of successful runs)
- Data quality metrics (duplicates, missing data, etc.)

### Alerting Thresholds
- **Critical:** Processing lag > 30 minutes
- **Warning:** Success rate < 95%
- **Info:** Unusual volume changes (>2 standard deviations)

## 🔗 Integration Examples

### With dbt Cloud
```yaml
# Use in dbt Cloud jobs for scheduled CDC processing
triggers:
  - type: schedule
    cron: "*/15 * * * *"  # Every 15 minutes
```

### With Airflow
```python
# Airflow DAG for CDC orchestration
cdc_dag = DAG(
    'cdc_processing',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    catchup=False
)
```

### With Event-Driven Processing
```sql
-- Process CDC changes triggered by events
SELECT * FROM changes
WHERE event_time >= '{{ var("event_start_time") }}'
  AND event_time < '{{ var("event_end_time") }}'
```

## 🎯 Next Steps

1. **Start Small:** Begin with timestamp-based CDC for a single table
2. **Add Monitoring:** Implement basic health checks and alerting
3. **Scale Up:** Gradually add more complex CDC patterns
4. **Optimize:** Fine-tune performance based on your specific requirements
5. **Productionize:** Add comprehensive monitoring and error handling

## 📖 Additional Resources

- [Official dbt Documentation - Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt Utils Package](https://github.com/dbt-labs/dbt-utils)
- [Change Data Capture Best Practices](https://konradkolenda.github.io/dbt/dbt_change_data_capture_guide.html)

## 🤝 Contributing

Found improvements or additional patterns? Please contribute back to help the community:

1. Test your changes thoroughly
2. Add appropriate documentation
3. Follow existing code patterns
4. Include performance considerations

---

*These examples are designed to be adapted to your specific use cases and data warehouse platform. Always test thoroughly in a development environment before deploying to production.*