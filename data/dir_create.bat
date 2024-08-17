@echo off

:: Create folders 
mkdir F:\hadoop\airflow_env\dags
mkdir F:\hadoop\airflow_env\data
mkdir F:\hadoop\airflow_env\db
mkdir F:\hadoop\airflow_env\scriptspy

:: Create files in each folder
echo. > F:\hadoop\airflow_env\dags\af01_analysis_pipeline.py
echo. > F:\hadoop\airflow_env\scriptspy\descriptive_analysis.py
echo. > F:\hadoop\airflow_env\scriptspy\preprocess_data.py
echo. > F:\hadoop\airflow_env\scriptspy\qualitative_analysis.py
echo. > F:\hadoop\airflow_env\scriptspy\train_ml_model.py
echo. > F:\hadoop\airflow_env\scriptspy\FraudAnalysisDriver.java
echo. > F:\hadoop\airflow_env\scriptspy\FraudAnalysisMapper.java
echo. > F:\hadoop\airflow_env\scriptspy\FraudAnalysisReducer.java

:: Create root files
echo. > F:\hadoop\airflow_env\.gitignore
echo. > F:\hadoop\airflow_env\airflow.cfg
echo. > F:\hadoop\airflow_env\README.md
echo. > F:\hadoop\airflow_env\requirements.txt

echo Pastas e arquivos criados com sucesso!