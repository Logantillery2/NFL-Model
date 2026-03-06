"# NFL-Model" 
# NFL Data Explorer Dashboard

This project builds a full data pipeline and interactive dashboard for exploring NFL game data.

## Pipeline

The pipeline automatically:

1. Fetches NFL data from the ESPN API
2. Cleans and standardizes each dataset
3. Combines them into a unified dataset
4. Outputs the final dataset

Final dataset:

data/unified/nfl_unified.csv

## Automation

The pipeline runs automatically using GitHub Actions.

## Dashboard

A Streamlit web application allows users to explore the data interactively.

Features include:

• Season range slider  
• Week range slider  
• Team comparison tools  
• Offensive vs defensive performance analysis  
• Home vs away scoring trends  
• Team ranking tables  

## Run Locally

Install dependencies:
pip install -r requirements.txt
streamlit run app.py
python run_pipeline.py
