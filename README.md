# Streamlit Web Application for Data Analysis

## Overview

This project is a Streamlit web application that connects to a PostgreSQL database to perform data analysis and visualization. It includes functionalities for modeling table relationships and generating reports with various metrics and visualizations.

## Features

- **Table Relationship & Filter Tool:** Allows users to select and join tables from a PostgreSQL database and view the joined results.
- **Pension Analytics Dashboard:** Provides a dashboard for analyzing pension claims data, including metrics and visualizations such as monthly claims, claims by status, and claims by stage.

## Installation

To run this application locally, follow these steps:

1. **Clone the Repository:**

   ```sh
   git clone https://github.com/Fadhili-Ibra/Streamlit-app-integrating-PostgreSQL-database.git

2. **Create and Activate a Virtual Environment:**

    ```sh
   python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`

**Install Required Packages:**

  ```sh
   pip install streamlit pandas psycopg2-binary sqlalchemy plotly

