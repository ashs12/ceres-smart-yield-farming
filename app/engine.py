import requests 
import os
import google.generativeai as genai
from dotenv import load_dotenv
from pyspark.sql  import SparkSession

load_dotenv()

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def get_live_weather(lat, lon, api_key):
    
    if not api_key:
        return {"temp": "N/A", "humidity": "N/A", "description": "Key Missing"}

    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        return {"temp": "N/A", "humidity": "N/A", "description": "API Error"}
    except Exception as e:
        print(f"Error fetching weather: {e}")
        return {"temp": "N/A", "humidity": "N/A", "description": "Connection Error"}

def build_rag_context(farm_id, gold_status_df, weather_data):

    farm_row = gold_status_df.filter(gold_status_df.farm_id == farm_id).collect()[0]

    context = f"""

            CONTEXT FOR AI AGRONOMIST:

            Farm ID: {farm_id}
    Crop: {farm_row['label']}
    Region: {farm_row['region']}
    
    HISTORICAL AVERAGES (Last 20 Days):
    - Temp: {farm_row['temperature']:.1f}°C
    - Soil pH: {farm_row['ph']:.1f}
    
    LIVE DATA:
    - Current Temp: {weather_data['main']['temp']}°C
    - Conditions: {weather_data['weather'][0]['description']}
    - Humidity: {weather_data['main']['humidity']}%
"""
    return context


def get_ai_advice(context):
    
    model = genai.GenerativeModel('gemini-2.5-flash')

    prompt = f""" 

        You are an expert AI Agronomist assisting farmers. 
        Using the data provided below, provide a short, actionable 3-bullet point advice for the farmer.
        Focus on specific interventions based on the pH and temperature vs. historical averages.
        {context}
"""
    response = model.generate_content(prompt)
    return response.text




