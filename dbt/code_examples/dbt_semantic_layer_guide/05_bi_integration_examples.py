#!/usr/bin/env python3
"""
BI Integration Examples for dbt Semantic Layer
Comprehensive Python client and integration patterns for MetricFlow API

This module demonstrates enterprise-grade integration patterns for consuming
dbt Semantic Layer metrics through various BI tools and custom applications.
"""

import json
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Union, Any
import logging
from dataclasses import dataclass
from abc import ABC, abstractmethod
import asyncio
import aiohttp
from urllib.parse import urljoin
import plotly.graph_objects as go
import plotly.express as px

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class MetricQuery:
    """Data class for metric query parameters"""
    metrics: List[str]
    dimensions: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    granularity: str = "day"
    filters: Optional[List[str]] = None
    limit: Optional[int] = None
    order_by: Optional[List[str]] = None


class SemanticLayerClient:
    """
    Comprehensive client for dbt Semantic Layer API
    Supports both GraphQL and REST endpoints with enterprise features
    """
    
    def __init__(self, base_url: str = "http://localhost:8080", api_key: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.graphql_endpoint = f"{base_url}/api/graphql"
        self.rest_endpoint = f"{base_url}/api/v1"
        self.session = requests.Session()
        
        # Configure authentication if API key provided
        if api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            })
    
    def health_check(self) -> bool:
        """Check if the semantic layer API is healthy"""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=10)
            return response.status_code == 200
        except requests.RequestException as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def list_metrics(self) -> Dict[str, Any]:
        """Get all available metrics from the semantic layer"""
        query = """
        query {
            metrics {
                name
                label
                description
                type
                semanticModel {
                    name
                }
                measures {
                    name
                    agg
                }
            }
        }
        """
        return self._execute_graphql_query(query)
    
    def list_semantic_models(self) -> Dict[str, Any]:
        """Get all available semantic models"""
        query = """
        query {
            semanticModels {
                name
                description
                entities {
                    name
                    type
                }
                dimensions {
                    name
                    type
                }
                measures {
                    name
                    agg
                    description
                }
            }
        }
        """
        return self._execute_graphql_query(query)
    
    def query_metrics(self, query_params: MetricQuery) -> pd.DataFrame:
        """
        Query metrics and return results as pandas DataFrame
        
        Args:
            query_params: MetricQuery object with query specifications
            
        Returns:
            pandas DataFrame with metric results
        """
        # Build GraphQL query
        metrics_list = json.dumps(query_params.metrics)
        dimensions_list = json.dumps(query_params.dimensions or [])
        filters_list = json.dumps(query_params.filters or [])
        
        query = f"""
        query {{
            metrics(names: {metrics_list}) {{
                name
                values(
                    dimensions: {dimensions_list}
                    timeRange: {{
                        start: "{query_params.start_date or '2023-01-01'}"
                        end: "{query_params.end_date or date.today().isoformat()}"
                    }}
                    granularity: "{query_params.granularity}"
                    {f'filters: {filters_list}' if query_params.filters else ''}
                    {f'limit: {query_params.limit}' if query_params.limit else ''}
                    {f'orderBy: {json.dumps(query_params.order_by)}' if query_params.order_by else ''}
                ) {{
                    {''.join([f'{dim}' for dim in query_params.dimensions or []])}
                    {''.join([f'{metric}' for metric in query_params.metrics])}
                    __timestamp
                }}
            }}
        }}
        """
        
        result = self._execute_graphql_query(query)
        return self._parse_query_result_to_dataframe(result)
    
    def query_metrics_with_comparison(self, 
                                    query_params: MetricQuery,
                                    comparison_period: str = "previous_period") -> pd.DataFrame:
        """
        Query metrics with period-over-period comparison
        
        Args:
            query_params: Base query parameters
            comparison_period: Type of comparison ('previous_period', 'year_over_year')
        """
        # Calculate comparison date range
        start_date = datetime.fromisoformat(query_params.start_date)
        end_date = datetime.fromisoformat(query_params.end_date)
        period_length = end_date - start_date
        
        if comparison_period == "previous_period":
            comp_start = start_date - period_length
            comp_end = start_date - timedelta(days=1)
        elif comparison_period == "year_over_year":
            comp_start = start_date - timedelta(days=365)
            comp_end = end_date - timedelta(days=365)
        else:
            raise ValueError(f"Unsupported comparison period: {comparison_period}")
        
        # Query current period
        current_data = self.query_metrics(query_params)
        current_data['period'] = 'current'
        
        # Query comparison period
        comp_query = MetricQuery(
            metrics=query_params.metrics,
            dimensions=query_params.dimensions,
            start_date=comp_start.isoformat(),
            end_date=comp_end.isoformat(),
            granularity=query_params.granularity,
            filters=query_params.filters
        )
        comp_data = self.query_metrics(comp_query)
        comp_data['period'] = 'comparison'
        
        # Combine results
        combined = pd.concat([current_data, comp_data], ignore_index=True)
        return combined
    
    def _execute_graphql_query(self, query: str) -> Dict[str, Any]:
        """Execute GraphQL query against the semantic layer"""
        payload = {"query": query}
        
        try:
            response = self.session.post(
                self.graphql_endpoint,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            if 'errors' in result:
                raise ValueError(f"GraphQL errors: {result['errors']}")
            
            return result.get('data', {})
            
        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def _parse_query_result_to_dataframe(self, result: Dict[str, Any]) -> pd.DataFrame:
        """Parse GraphQL query result into pandas DataFrame"""
        if not result.get('metrics'):
            return pd.DataFrame()
        
        # Extract data from nested structure
        all_rows = []
        for metric_data in result['metrics']:
            metric_name = metric_data['name']
            for row in metric_data.get('values', []):
                row_data = row.copy()
                row_data['metric_name'] = metric_name
                all_rows.append(row_data)
        
        if not all_rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_rows)
        
        # Convert timestamp to datetime if present
        if '__timestamp' in df.columns:
            df['__timestamp'] = pd.to_datetime(df['__timestamp'])
            df = df.rename(columns={'__timestamp': 'timestamp'})
        
        return df


class TableauIntegration:
    """Integration patterns for Tableau with dbt Semantic Layer"""
    
    def __init__(self, semantic_client: SemanticLayerClient):
        self.client = semantic_client
    
    def generate_tableau_extract(self, query_params: MetricQuery, 
                                output_path: str = "semantic_layer_extract.tde"):
        """
        Generate Tableau Data Extract (TDE) file from semantic layer
        Requires tableauserverclient library: pip install tableauserverclient
        """
        try:
            import tableausdk
            from tableausdk import *
            from tableausdk.HyperExtract import *
        except ImportError:
            logger.error("Tableau SDK not available. Install tableauserverclient package.")
            return None
        
        # Query data from semantic layer
        df = self.client.query_metrics(query_params)
        
        if df.empty:
            logger.warning("No data returned from semantic layer query")
            return None
        
        # Create Tableau extract
        try:
            # Initialize extract
            ExtractAPI.initialize()
            extract = Extract(output_path)
            
            # Define table schema based on DataFrame
            table_def = TableDefinition()
            
            # Add columns based on DataFrame structure
            for column in df.columns:
                if df[column].dtype in ['int64', 'float64']:
                    table_def.addColumn(column, Type.DOUBLE)
                elif df[column].dtype == 'datetime64[ns]':
                    table_def.addColumn(column, Type.DATETIME)
                else:
                    table_def.addColumn(column, Type.UNICODE_STRING)
            
            # Create table
            table = extract.addTable("SemanticLayerData", table_def)
            
            # Insert data
            for _, row in df.iterrows():
                new_row = Row(table_def)
                for i, column in enumerate(df.columns):
                    value = row[column]
                    if pd.isna(value):
                        continue
                    
                    if df[column].dtype in ['int64', 'float64']:
                        new_row.setDouble(i, float(value))
                    elif df[column].dtype == 'datetime64[ns]':
                        new_row.setDateTime(i, int(value.timestamp()))
                    else:
                        new_row.setString(i, str(value))
                
                table.insert(new_row)
            
            extract.close()
            ExtractAPI.cleanup()
            
            logger.info(f"Tableau extract created: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to create Tableau extract: {e}")
            return None
    
    def generate_tableau_datasource_xml(self, query_params: MetricQuery) -> str:
        """Generate Tableau data source XML for live connection"""
        
        # Query sample data to understand structure
        df = self.client.query_metrics(query_params)
        
        xml_template = """<?xml version='1.0' encoding='utf-8' ?>
<datasource formatted-name='dbt Semantic Layer' inline='true' source-platform='win' version='18.1'>
  <connection class='genericodbc' dbname='' odbc-connect-string-extras='' server='localhost' username='' workgroup-auth-mode='as-is' />
  <aliases enabled='yes' />
  <column caption='Metric Time' datatype='datetime' name='[metric_time]' role='dimension' type='ordinal' />
  {columns}
  <layout dim-ordering='alphabetic' dim-percentage='0.5' measure-ordering='alphabetic' measure-percentage='0.4' show-structure='true' />
  <semantic-values>
    <semantic-value key='[Country].[Name]' value='&quot;United States&quot;' />
  </semantic-values>
</datasource>"""
        
        columns_xml = ""
        for column in df.columns:
            if column != 'timestamp':
                if df[column].dtype in ['int64', 'float64']:
                    role = 'measure'
                    datatype = 'real'
                else:
                    role = 'dimension'
                    datatype = 'string'
                
                columns_xml += f'  <column caption="{column.title()}" datatype="{datatype}" name="[{column}]" role="{role}" />\n'
        
        return xml_template.format(columns=columns_xml)


class LookerIntegration:
    """Integration patterns for Looker with dbt Semantic Layer"""
    
    def __init__(self, semantic_client: SemanticLayerClient):
        self.client = semantic_client
    
    def generate_looker_view(self, semantic_model_name: str) -> str:
        """Generate Looker view LookML based on semantic model"""
        
        # Get semantic model metadata
        models_data = self.client.list_semantic_models()
        semantic_model = None
        
        for model in models_data.get('semanticModels', []):
            if model['name'] == semantic_model_name:
                semantic_model = model
                break
        
        if not semantic_model:
            raise ValueError(f"Semantic model '{semantic_model_name}' not found")
        
        lookml_template = '''view: {view_name} {{
  sql_table_name: semantic_layer_query ;;
  
  # Dimensions
{dimensions}
  
  # Measures  
{measures}
  
  # Sets
  set: default_fields {{
    fields: [
{default_fields}
    ]
  }}
}}'''
        
        # Generate dimensions
        dimensions_lookml = ""
        for dim in semantic_model.get('dimensions', []):
            dim_type = 'string'
            if dim['type'] == 'time':
                dim_type = 'datetime'
            elif dim['type'] in ['number', 'numeric']:
                dim_type = 'number'
            
            dimensions_lookml += f'''  dimension: {dim['name']} {{
    type: {dim_type}
    sql: ${{TABLE}}.{dim['name']} ;;
  }}
  
'''
        
        # Generate measures
        measures_lookml = ""
        default_fields = []
        
        for measure in semantic_model.get('measures', []):
            agg_type = measure.get('agg', 'sum').lower()
            
            measures_lookml += f'''  measure: {measure['name']} {{
    type: {agg_type}
    sql: ${{TABLE}}.{measure['name']} ;;
    description: "{measure.get('description', '')}"
  }}
  
'''
            default_fields.append(f'      {measure["name"]}')
        
        # Add key dimensions to default fields
        for dim in semantic_model.get('dimensions', [])[:3]:  # First 3 dimensions
            default_fields.append(f'      {dim["name"]}')
        
        return lookml_template.format(
            view_name=semantic_model_name,
            dimensions=dimensions_lookml,
            measures=measures_lookml,
            default_fields=',\n'.join(default_fields)
        )
    
    def create_looker_dashboard(self, metrics: List[str], 
                              dashboard_title: str = "Semantic Layer Dashboard") -> str:
        """Generate Looker dashboard LookML"""
        
        dashboard_lookml = f'''- dashboard: semantic_layer_dashboard
  title: {dashboard_title}
  layout: newspaper
  preferred_viewer: dashboards-next
  
  elements:
'''
        
        # Generate dashboard elements for each metric
        for i, metric in enumerate(metrics):
            dashboard_lookml += f'''  - title: {metric.replace('_', ' ').title()}
    name: {metric}_chart
    model: semantic_layer
    explore: metrics
    type: looker_line
    fields: [metrics.metric_time, metrics.{metric}]
    fill_fields: [metrics.metric_time]
    sorts: [metrics.metric_time desc]
    limit: 500
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: ''
    limit_displayed_rows: false
    legend_position: center
    point_style: none
    show_value_labels: false
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    show_null_points: true
    interpolation: linear
    row: {i * 8}
    col: 0
    width: 12
    height: 8

'''
        
        return dashboard_lookml


class PowerBIIntegration:
    """Integration patterns for Microsoft Power BI"""
    
    def __init__(self, semantic_client: SemanticLayerClient):
        self.client = semantic_client
    
    def generate_powerbi_m_query(self, query_params: MetricQuery) -> str:
        """Generate Power Query M language code for Power BI"""
        
        # Convert query parameters to GraphQL
        metrics_json = json.dumps(query_params.metrics)
        dimensions_json = json.dumps(query_params.dimensions or [])
        
        graphql_query = f'''{{
  metrics(names: {metrics_json}) {{
    name
    values(
      dimensions: {dimensions_json}
      timeRange: {{
        start: "{query_params.start_date or '2023-01-01'}"
        end: "{query_params.end_date or date.today().isoformat()}"
      }}
      granularity: "{query_params.granularity}"
    ) {{
      {''.join([f'{dim} ' for dim in query_params.dimensions or []])}
      {''.join([f'{metric} ' for metric in query_params.metrics])}
      __timestamp
    }}
  }}
}}'''
        
        m_query = f'''let
    BaseUrl = "{self.client.base_url}/api/graphql",
    Query = "{graphql_query.replace('"', '""').replace(chr(10), '').replace(chr(13), '')}",
    
    RequestBody = "{{ ""query"": """ & Query & """ }}",
    
    Response = Web.Contents(BaseUrl, [
        Headers = [
            #"Content-Type" = "application/json",
            #"Authorization" = "Bearer YOUR_API_KEY_HERE"
        ],
        Content = Text.ToBinary(RequestBody)
    ]),
    
    JsonData = Json.Document(Response),
    Data = JsonData[data],
    Metrics = Data[metrics],
    
    // Convert nested structure to flat table
    ExpandedData = Table.ExpandListColumn(
        Table.FromList(Metrics, Splitter.SplitByNothing()),
        "Column1"
    ),
    
    ExpandedValues = Table.ExpandRecordColumn(
        ExpandedData,
        "Column1",
        {{"name", "values"}},
        {{"metric_name", "values"}}
    ),
    
    ExpandedRows = Table.ExpandListColumn(
        ExpandedValues,
        "values"
    ),
    
    FinalTable = Table.ExpandRecordColumn(
        ExpandedRows,
        "values",
        List.Union({{
            {json.dumps(query_params.dimensions or [])},
            {json.dumps(query_params.metrics)},
            {{"__timestamp"}}
        }}),
        List.Union({{
            {json.dumps(query_params.dimensions or [])},
            {json.dumps(query_params.metrics)},
            {{"timestamp"}}
        }})
    ),
    
    // Convert timestamp to datetime
    ConvertedTypes = Table.TransformColumnTypes(
        FinalTable,
        {{"timestamp", type datetime}}
    )
in
    ConvertedTypes'''
        
        return m_query
    
    def create_powerbi_dataset_json(self, semantic_model_name: str) -> Dict[str, Any]:
        """Create Power BI dataset definition JSON"""
        
        # Get semantic model metadata
        models_data = self.client.list_semantic_models()
        semantic_model = None
        
        for model in models_data.get('semanticModels', []):
            if model['name'] == semantic_model_name:
                semantic_model = model
                break
        
        if not semantic_model:
            raise ValueError(f"Semantic model '{semantic_model_name}' not found")
        
        # Build dataset definition
        dataset = {
            "name": f"dbt_semantic_layer_{semantic_model_name}",
            "tables": [
                {
                    "name": semantic_model_name,
                    "columns": [],
                    "measures": []
                }
            ],
            "relationships": []
        }
        
        table = dataset["tables"][0]
        
        # Add dimensions as columns
        for dim in semantic_model.get('dimensions', []):
            column_type = "string"
            if dim['type'] == 'time':
                column_type = "dateTime"
            elif dim['type'] in ['number', 'numeric']:
                column_type = "double"
            
            table["columns"].append({
                "name": dim['name'],
                "dataType": column_type,
                "isHidden": False
            })
        
        # Add measures
        for measure in semantic_model.get('measures', []):
            table["measures"].append({
                "name": measure['name'],
                "expression": f"SUM([{measure['name']}])",
                "description": measure.get('description', ''),
                "isHidden": False
            })
        
        return dataset


class SemanticLayerAnalytics:
    """Advanced analytics and visualization utilities"""
    
    def __init__(self, semantic_client: SemanticLayerClient):
        self.client = semantic_client
    
    def create_metric_dashboard(self, metrics: List[str], 
                              dimensions: List[str] = None,
                              start_date: str = None,
                              end_date: str = None) -> go.Figure:
        """Create interactive Plotly dashboard for metrics"""
        
        query_params = MetricQuery(
            metrics=metrics,
            dimensions=dimensions or ['order_date'],
            start_date=start_date or (date.today() - timedelta(days=30)).isoformat(),
            end_date=end_date or date.today().isoformat(),
            granularity="day"
        )
        
        df = self.client.query_metrics(query_params)
        
        if df.empty:
            logger.warning("No data available for dashboard")
            return go.Figure()
        
        # Create subplots for multiple metrics
        from plotly.subplots import make_subplots
        
        fig = make_subplots(
            rows=len(metrics), 
            cols=1,
            subplot_titles=metrics,
            shared_xaxes=True,
            vertical_spacing=0.1
        )
        
        # Add trace for each metric
        for i, metric in enumerate(metrics):
            metric_data = df[df['metric_name'] == metric]
            
            fig.add_trace(
                go.Scatter(
                    x=metric_data['timestamp'],
                    y=metric_data[metric],
                    mode='lines+markers',
                    name=metric,
                    line=dict(width=2),
                    hovertemplate=f'{metric}: %{{y}}<br>Date: %{{x}}<extra></extra>'
                ),
                row=i+1, col=1
            )
        
        # Update layout
        fig.update_layout(
            height=300 * len(metrics),
            title="Semantic Layer Metrics Dashboard",
            showlegend=False,
            hovermode='x unified'
        )
        
        return fig
    
    def analyze_metric_trends(self, metric: str, 
                            dimension: str = None,
                            lookback_days: int = 90) -> Dict[str, Any]:
        """Perform trend analysis on a metric"""
        
        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)
        
        query_params = MetricQuery(
            metrics=[metric],
            dimensions=[dimension] if dimension else ['order_date'],
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            granularity="day"
        )
        
        df = self.client.query_metrics(query_params)
        
        if df.empty:
            return {"error": "No data available for analysis"}
        
        # Calculate trend statistics
        values = df[metric].values
        trend_analysis = {
            "metric": metric,
            "period_days": lookback_days,
            "total_value": float(values.sum()),
            "average_value": float(values.mean()),
            "min_value": float(values.min()),
            "max_value": float(values.max()),
            "std_deviation": float(values.std()),
            "coefficient_of_variation": float(values.std() / values.mean()) if values.mean() > 0 else 0,
            "trend_direction": "increasing" if values[-7:].mean() > values[:7].mean() else "decreasing",
            "volatility": "high" if values.std() / values.mean() > 0.3 else "normal"
        }
        
        # Calculate period-over-period change
        if len(values) >= 14:
            recent_avg = values[-7:].mean()
            previous_avg = values[-14:-7].mean()
            
            trend_analysis["week_over_week_change"] = (recent_avg - previous_avg) / previous_avg * 100
        
        return trend_analysis


# Example Usage and Testing
if __name__ == "__main__":
    # Initialize semantic layer client
    client = SemanticLayerClient(base_url="http://localhost:8080")
    
    # Check health
    if not client.health_check():
        print("Semantic Layer API is not available")
        exit(1)
    
    print("✅ Semantic Layer API is healthy")
    
    # List available metrics
    metrics_data = client.list_metrics()
    print(f"📊 Found {len(metrics_data.get('metrics', []))} metrics")
    
    # Example metric query
    query = MetricQuery(
        metrics=["total_revenue", "order_count"],
        dimensions=["customer_region", "sales_channel"],
        start_date="2024-01-01",
        end_date="2024-12-31",
        granularity="month"
    )
    
    # Execute query
    results = client.query_metrics(query)
    print(f"📈 Query returned {len(results)} rows")
    print(results.head())
    
    # Generate integrations
    tableau_integration = TableauIntegration(client)
    looker_integration = LookerIntegration(client)
    powerbi_integration = PowerBIIntegration(client)
    
    # Generate Looker view
    try:
        looker_view = looker_integration.generate_looker_view("orders")
        print("🔍 Generated Looker view LookML")
    except Exception as e:
        print(f"❌ Failed to generate Looker view: {e}")
    
    # Generate Power BI M query
    powerbi_query = powerbi_integration.generate_powerbi_m_query(query)
    print("⚡ Generated Power BI M query")
    
    # Create analytics dashboard
    analytics = SemanticLayerAnalytics(client)
    trend_analysis = analytics.analyze_metric_trends("total_revenue", lookback_days=30)
    print(f"📊 Trend Analysis: {trend_analysis.get('trend_direction', 'unknown')} trend")
    
    print("🚀 All integration examples completed successfully!")