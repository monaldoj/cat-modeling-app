# Catastrophe Risk Modeling App

A geospatial visualization application for modeling and analyzing catastrophe risks using H3 hexagonal grids. Built with Dash and deployed on Databricks, this interactive web application allows users to explore various catastrophe risk scores across different geographic regions.

## Features

### 🗺️ Interactive Map Visualization
- **H3 Hexagonal Grid Display**: Visualize risk data using Uber's H3 geospatial indexing system
- **Multiple Resolution Levels**: Dynamic resolution adjustment based on zoom level and viewport
- **Dark Mode Map**: Sleek CartoDB dark theme for better contrast and visualization
- **Responsive Zoom & Pan**: Smooth navigation with automatic data refresh

### 📊 Catastrophe Risk Types
The application supports 19 different catastrophe risk types:
- Flood Risk
- Avalanche Risk
- Coastal Flood Risk
- Cold Wave Risk
- Drought Risk
- Earthquake Risk
- Hail Risk
- Heat Wave Risk
- Hurricane Risk
- Ice Storm Risk
- Landslide Risk
- Lightning Risk
- River Flood Risk
- Strong Wind Risk
- Tornado Risk
- Tsunami Risk
- Volcano Risk
- Wildfire Risk
- Winter Weather Risk

### 🎯 Event-Based Analysis
- Browse and visualize specific catastrophe events
- View event boundaries and affected areas
- Compare historical event patterns

### ⚙️ Advanced Features
- **Dynamic Data Loading**: Efficient querying with viewport-based filtering
- **H3 Spatial Functions**: Leverages Databricks H3 functions for spatial operations
- **Real-time Updates**: Map automatically updates when panning, zooming, or changing risk types
- **Color-Coded Risk Levels**: Intuitive color gradients from low to high risk

## Architecture

### Technology Stack
- **Frontend**: 
  - [Dash](https://dash.plotly.com/) - Python web framework
  - [Dash Leaflet](https://dash-leaflet.com/) - Interactive maps
  - [Dash Bootstrap Components](https://dash-bootstrap-components.opensource.faculty.ai/) - UI components
  - [Dash AG Grid](https://dash.plotly.com/dash-ag-grid) - Data tables
  - [Plotly](https://plotly.com/) - Data visualization

- **Backend**:
  - Python 3.x
  - Databricks SQL Connector
  - Databricks SDK

- **Data Storage**:
  - Databricks Delta Lake
  - H3 indexed geospatial data
  
- **Deployment**:
  - Databricks Apps
  - Databricks Asset Bundles (DAB)

### Data Model
The application queries data from:
- **Catalog**: `timo`
- **Schema**: `cat_risk`
- **Main Table**: `cat_risk_scores_h3`
- **Events Table**: `cat_events`

## Prerequisites

- Python 3.8 or higher
- Databricks workspace with SQL Warehouse access
- Databricks Personal Access Token (for local development)
- Access to the required Databricks catalog and schemas

## Installation

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd cat-modeling-app
   ```

2. **Install dependencies**
   ```bash
   # Using pip
   pip install -r app/requirements.txt
   
   # Or using uv (faster)
   uv pip install -r app/requirements.txt
   ```

3. **Set up environment variables**
   
   Create a `.env` file in the project root:
   ```env
   DATABRICKS_HOST=<your-databricks-host>
   DATABRICKS_TOKEN=<your-personal-access-token>
   DATABRICKS_WAREHOUSE_ID=<your-sql-warehouse-id>
   ```

4. **Run the application**
   ```bash
   cd app
   python app.py
   ```

5. **Access the application**
   
   Open your browser and navigate to `http://localhost:8050`

### Deployment to Databricks

This project uses Databricks Asset Bundles (DAB) for deployment.

1. **Configure the bundle**
   
   Edit `databricks.yml` and `resources/cat-modeling-app.yml` with your workspace details:
   - SQL Warehouse ID
   - Budget policy ID (optional)
   - App name and description

2. **Deploy to development**
   ```bash
   databricks bundle deploy
   ```

3. **Deploy to production**
   ```bash
   databricks bundle deploy --target prod
   ```

4. **Access the deployed app**
   
   The app will be available through your Databricks workspace Apps section.

## Project Structure

```
cat-modeling-app/
├── app/
│   ├── app.py              # Main application code
│   ├── app.yml             # App configuration
│   └── requirements.txt    # Python dependencies
├── resources/
│   └── cat-modeling-app.yml # Databricks App resource definition
├── databricks.yml          # DAB configuration
├── .gitignore             # Git ignore rules
└── README.md              # This file
```

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRICKS_HOST` | Databricks workspace URL | Yes (local dev) |
| `DATABRICKS_TOKEN` | Personal access token or uses on-behalf-of auth | Yes (local dev) |
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse ID for queries | Yes |

### Application Settings

Key configuration in `app/app.py`:
- `default_catalog`: Default Databricks catalog (default: `timo`)
- `default_schema`: Default schema (default: `cat_risk`)
- `default_table`: Default table (default: `cat_risk_scores_h3`)
- `default_catastrophe_type`: Default risk type to display (default: `flood_risk`)

## Usage

1. **Select a Risk Type**: Use the dropdown menu to choose which catastrophe risk to visualize
2. **Navigate the Map**: Pan and zoom to explore different geographic regions
3. **View Events**: Browse and select specific catastrophe events to see their boundaries
4. **Adjust Detail**: The map automatically adjusts hexagon resolution based on your zoom level

## Data Requirements

The application expects data in the following format:

### Risk Scores Table (`cat_risk_scores_h3`)
- H3 index column (typically at high resolution)
- Risk score columns for each catastrophe type (float values)
- Supports H3 spatial functions for aggregation

### Events Table (`cat_events`)
- `event_name`: Name of the catastrophe event
- `wkt`: Well-Known Text representation of event boundary
- Additional event metadata

## Development

### Adding New Risk Types

To add a new catastrophe risk type:

1. Add the risk type to the `catastrophe_types` list in `app.py`
2. Ensure the corresponding column exists in your data table
3. The dropdown will automatically populate with the new option

### Customizing the Map

- **Tile Layer**: Modify `tile_layer_url` in `app.py`
- **Color Scheme**: Adjust the Plotly color scale in the choropleth layer
- **Initial View**: Set `global_center` and `global_zoom` variables

## Performance Considerations

- The application uses viewport-based filtering to limit data queries
- H3 resolution automatically adjusts based on zoom level to balance detail vs. performance
- Queries leverage Databricks H3 functions for efficient spatial operations
- Data is aggregated at appropriate resolutions to reduce payload size

## Troubleshooting

### Connection Issues
- Verify `DATABRICKS_WAREHOUSE_ID` is set correctly
- Check that your Databricks token has SQL warehouse access permissions
- Ensure the SQL warehouse is running

### Missing Data
- Confirm the catalog, schema, and table names match your Databricks environment
- Verify the H3 column name matches your data structure
- Check that risk score columns exist for selected catastrophe types

### Performance Issues
- Consider reducing the initial data load by setting tighter bounds
- Adjust H3 resolution thresholds for your use case
- Ensure your SQL warehouse has adequate capacity

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

[Specify your license here]

## Contact

[Add contact information or links to relevant documentation]

## Acknowledgments

- Built with [Databricks](https://databricks.com/)
- Powered by [H3 Geospatial Indexing](https://h3geo.org/)
- UI components from [Dash](https://dash.plotly.com/)
- Map visualization with [Leaflet](https://leafletjs.com/)

