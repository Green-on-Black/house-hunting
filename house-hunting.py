import requests
import re
import calendar
import csv
import json
import os
from bs4 import BeautifulSoup
from datetime import date, datetime
from dotenv import load_dotenv
from io import StringIO

# Load variables from the .env file
load_dotenv()

# =========================================================================
# 1. CONFIGURATION
# =========================================================================
GRIST_HOST = os.environ.get("GRIST_HOST")
GRIST_API_KEY = os.environ.get("GRIST_API_KEY")
GRIST_DOC_ID = os.environ.get("GRIST_DOC_ID")
GRIST_MARKET_TABLE_ID = os.environ.get("GRIST_MARKET_TABLE_ID")
CF_ACCESS_CLIENT_ID = os.environ.get("CF_ACCESS_CLIENT_ID")
CF_ACCESS_CLIENT_SECRET = os.environ.get("CF_ACCESS_CLIENT_SECRET")

# Import Redfin data
with open('redfin_towns.json', 'r') as f:
	TOWNS_TO_TRACK = json.load(f)

# Import StreetEasy data (for NYC neighborhoods)
with open('streeteasy_neighborhoods.json', 'r') as f:
	NYC_NEIGHBORHOODS = set(json.load(f))

# Define ALL the column keys that exist in the Sales table
GRIST_MASTER_SCHEMA = [
	'Date',
	'Town',
	'Region',
	'Median_Sale_Price',
	'Median_List_Price',
	'Overall_Average_Premium_Paid',
	'Median_DOM',
	'Avg_Home_Premium',
	'Avg_Home_DOM',
	'Hot_Home_Premium',
	'Hot_Home_DOM',
	'Num_of_Homes_Sold',
	'Compete_Score'
]

# CSS selectors for Redfin Market Data
MARKET_SELECTORS = {
	'LONG_DATE': 'section.MarketInsightsSummarySection p',
	'SALE_PRICE': '#home_prices div.ModeToggler.dataTabs button.selected div.value',
	'SALE_TO_LIST_RATIO': '#compete div#demand.MarketInsightsGraphSection div.ModeToggler.dataTabs button.selected div.value',
	'MEDIAN_DOM': '#home_prices div.ModeToggler.dataTabs button:nth-child(3) div.value',
	'AVERAGE_AVERAGE_PREMIUM': '#compete > div.CompeteScoreSectionV2 > div > div > div.scoreDetails > ul > li:nth-child(2) > span > b:nth-child(1)',
	'AVERAGE_AVERAGE_DOM': '#compete > div.CompeteScoreSectionV2 > div > div > div.scoreDetails > ul > li:nth-child(2) > span > b:nth-child(2)',
	'HOT_AVERAGE_PREMIUM': '#compete > div.CompeteScoreSectionV2 > div > div > div.scoreDetails > ul > li:nth-child(3) > span > b:nth-child(1)',
	'HOT_AVERAGE_DOM': '#compete > div.CompeteScoreSectionV2 > div > div > div.scoreDetails > ul > li:nth-child(3) > span > b:nth-child(2)',
	'NUM_OF_HOMES_SOLD': '#home_prices > div.desktop-section-content > div.ModeToggler.dataTabs > button:nth-child(2) > div > div.dataPoints > div.value',
	'COMPETE_SCORE': '#compete > div.CompeteScoreSectionV2 > div > div > div.DemandRow--BarScore > div.score'
}

# StreetEasy CSV files
STREETEASY_CSV_URLS = {
	'STREETEASY_MEDIAN_ASKING_PRICE': 'https://cdn-charts.streeteasy.com/chart/v2/data/sub/ddp-medianAskingPrice.csv',
	'STREETEASY_SALE_TO_LIST_RATIO': 'https://cdn-charts.streeteasy.com/chart/v2/data/sub/ddp-saleListRatio.csv',
	'STREETEASY_MEDIAN_RECORDED_PRICE': 'https://cdn-charts.streeteasy.com/chart/v2/data/sub/ddp-medianRecordedSalesPrice.csv',
	'STREETEASY_MEDIAN_DOM': 'https://cdn-charts.streeteasy.com/chart/v2/data/sub/ddp-medianDaysMarket.csv',
	'STREETEASY_NUM_OF_HOMES_SOLD': 'https://cdn-charts.streeteasy.com/chart/v2/data/sub/ddp-recordedSales.csv'
}

# Define the columns in the CSV files supplied by StreetEasy
CSV_COL_INDEXES = {
    'DATE': 2, 
    'NEIGHBORHOOD': 0, 
    'VALUE': 4,
}

# =========================================================================
# 2. DATA RETRIEVAL FUNCTIONS
# =========================================================================

def get_last_day_of_month(month_year_str):
		"""
		Converts a 'Month YYYY' string (e.g., 'September 2025') into the M/D/YYYY format
		for the last day of that month (e.g., '9/30/2025').
		"""
		try:
				# 1. Parse the string into a datetime object
				# The locale might affect the month name, but generally works with English.
				dt = datetime.strptime(month_year_str, '%B %Y')

				# 2. Find the last day number of that month
				_, last_day = calendar.monthrange(dt.year, dt.month)

				# 3. Create the final date object (M/D/YYYY)
				final_date = date(dt.year, dt.month, last_day)

				return final_date.strftime('%#m/%#d/%Y') # %#m and %#d are for no leading zero (if supported)

		except ValueError as e:
				print(f"ERROR: Date parsing failed for '{month_year_str}': {e}")
				return None

def get_clean_number(element, default=0):
		"""Safely extracts, cleans, and converts text from a soup element to an integer or float."""
		if not element:
				return default

		text = element.get_text(strip=True)

		# Check if the text is a percentage (e.g., '103.0%') or a number (e.g., '65')
		if '%' in text:
				# For premiums: remove %, convert to float, divide by 100, then subtract 1.0 (and round)
				value = float(text.replace('%', '').replace('+', '').replace('-', '')) / 100
				return round(value - 1.0, 4)
		else:
				# For Compete Score/DOM: clean non-digits, then convert to int
				clean_text = re.sub(r'[^\d]', '', text)
				try:
						return int(clean_text)
				except ValueError:
						return default

def get_clean_premium_percentage(element, default=0.0):
		"""Safely extracts, cleans, and converts premium text (e.g., '+8%') to a float premium (e.g., 0.08)."""
		if not element:
				return default

		text = element.get_text(strip=True)

		try:
				# Remove % symbol
				clean_text = text.replace('%', '')

				# Check for '+' or '-' sign to determine if it's a premium or discount
				if '+' in clean_text:
						value = float(clean_text.replace('+', '')) / 100
				elif '-' in clean_text:
						value = float(clean_text) / 100
				else:
						# If no sign, assume it's a premium
						value = float(clean_text) / 100

				return round(value, 4)
		except ValueError:
				return default

def normalize_record_for_grist(record, schema):
	"""
	Ensures the record has all keys defined in the master schema,
	setting missing values to None.
	"""
	normalized_record = {}
	for key in schema:
		# Get the value if the key exists, otherwise use None
		normalized_record[key] = record.get(key, None)
	return normalized_record

def scrape_streeteasy_data(url_key, metric_key, target_towns, csv_indexes):
	"""Fetches a StreetEasy CSV, parses it, and extracts the latest data point for target towns."""
	
	url = STREETEASY_CSV_URLS[url_key]
	town_data = {town: {} for town in target_towns}
	
	try:
		response = requests.get(url, timeout=15)
		response.raise_for_status()
		
		# Use StringIO to treat the string content as a file
		data_file = StringIO(response.text)
		reader = csv.reader(data_file)
		
		# Skip header row (Assuming only one header row)
		next(reader) 

		for row in reader:
			try:
				# Use the provided column indexes
				town = row[csv_indexes['NEIGHBORHOOD']]
				date_str = row[csv_indexes['DATE']]
				value = row[csv_indexes['VALUE']]
			except IndexError:
				# Skip malformed rows
				continue

			if town in target_towns:
				# Date format in CSV is YYYY-MM-DD
				current_date = datetime.strptime(date_str, '%Y-%m-%d')
				
				# Logic to ensure we only store the LATEST data point
				if not town_data[town] or current_date > town_data[town].get('_DateTimeObject', datetime.min):
					
					# 1. Find the last day number of that month
					_, last_day = calendar.monthrange(current_date.year, current_date.month)
					
					# 2. Create the final date object (Last Day of Month)
					final_date = date(current_date.year, current_date.month, last_day)

					# 3. Format the final date as MM/DD/YYYY (e.g., '10/31/2025')
					formatted_date_str = final_date.strftime('%m/%d/%Y')

					town_data[town]['_DateTimeObject'] = current_date
					town_data[town]['Date'] = formatted_date_str
					town_data[town]['Town'] = town
					town_data[town]['Region'] = 'New York City' 
					town_data[town][metric_key] = value
					
		# Clean up and return
		for town in town_data:
			if '_DateTimeObject' in town_data[town]:
				del town_data[town]['_DateTimeObject']
				
		# Filter out towns where no data was found
		return {k: v for k, v in town_data.items() if v}

	except Exception as e:
		print(f"ERROR: StreetEasy CSV fetch/parse failed for {url_key}: {e}")
		return {}

def scrape_market_summary(town, region, city_url):
		"""
		Scrapes key market metrics (Median Sale Price, Sale-to-List Ratio, Hot/Avg Premiums)
		from a target city's Redfin market trends page.
		"""
		headers = {
				'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
		}

		print(f"Fetching market data for {town} from: {city_url}")

		try:
				response = requests.get(city_url, headers=headers, timeout=15)
				response.raise_for_status()
				soup = BeautifulSoup(response.content, 'html.parser')

				# --- Extract ALL Raw Data Elements (Allowing some to be None) ---
				long_date_elem = soup.select_one(MARKET_SELECTORS['LONG_DATE'])
				sale_price_elem = soup.select_one(MARKET_SELECTORS['SALE_PRICE'])
				ratio_elem = soup.select_one(MARKET_SELECTORS['SALE_TO_LIST_RATIO'])
				median_dom_elem = soup.select_one(MARKET_SELECTORS['MEDIAN_DOM'])
				avg_avg_premium_elem = soup.select_one(MARKET_SELECTORS['AVERAGE_AVERAGE_PREMIUM'])
				avg_avg_dom_elem = soup.select_one(MARKET_SELECTORS['AVERAGE_AVERAGE_DOM'])
				hot_avg_premium_elem = soup.select_one(MARKET_SELECTORS['HOT_AVERAGE_PREMIUM'])
				hot_avg_dom_elem = soup.select_one(MARKET_SELECTORS['HOT_AVERAGE_DOM'])
				num_of_homes_sold_elem = soup.select_one(MARKET_SELECTORS['NUM_OF_HOMES_SOLD'])
				compete_score_elem = soup.select_one(MARKET_SELECTORS['COMPETE_SCORE'])

				# --- START CORE DATA PROCESSING (Must succeed for data to be useful) ---

				# We put core processing in a try block to catch the "NoneType" error immediately
				try:
						# 1. Date Extraction (CRITICAL)
						if not long_date_elem:
							print("ERROR: Could not find the market summary paragraph for date extraction. Selector may be outdated.")
							return None

						long_date_text = long_date_elem.get_text(strip=True)

						# Regex to find the Month YYYY pattern exactly at the beginning of the text, 
						# after the word "In" (which is the first word you mentioned).
						# Example Text: "In October 2025, Ridgewood home prices were up..."
						# Regex: Finds "Month YYYY"
						match = re.search(r'In\s+([A-Za-z]+\s+\d{4})', long_date_text)

						if not match:
							print("ERROR: Could not extract Month YYYY string from summary text using regex.")
							return None

						month_year_str = match.group(1) # This reliably captures 'October 2025'
						formatted_date = get_last_day_of_month(month_year_str)

						if formatted_date is None:
							return None

						# 2. Median Sale Price Extraction (CRITICAL)
						sale_price_text = re.sub(r'[^\d]', '', sale_price_elem.get_text(strip=True)) # Fails if sale_price_elem is None
						median_sale_price = int(sale_price_text)

						# 3. Sale-to-List Ratio Extraction (CRITICAL)
						ratio_text = ratio_elem.get_text(strip=True).replace('%', '') # Fails if ratio_elem is None
						original_sale_to_list_ratio = float(ratio_text) / 100

				except AttributeError:
						# If any of the above core extractions failed, this block catches it
						print(f"ERROR: Core data (Price, Ratio, Date) missing for {town}. Skipping.")
						return None

				# 4. Calculated Core Metrics --- ALL BELOW ARE OPTIONAL ---
				median_list_price = int(median_sale_price / original_sale_to_list_ratio)
				overall_average_premium_paid = round(original_sale_to_list_ratio - 1.0, 4)

				# 5. Segmented Metrics (These use the safe helper functions and will return defaults if elements are None)
				median_dom = get_clean_number(median_dom_elem, default=0)
				avg_home_premium = get_clean_premium_percentage(avg_avg_premium_elem, default=0.0)
				avg_home_dom = get_clean_number(avg_avg_dom_elem, default=0)
				hot_home_premium = get_clean_premium_percentage(hot_avg_premium_elem, default=0.0)
				hot_home_dom = get_clean_number(hot_avg_dom_elem, default=0)
				compete_score = get_clean_number(compete_score_elem, default=0)

				if num_of_homes_sold_elem:
					num_homes_sold = num_of_homes_sold_elem.text.strip()
				else:
					num_homes_sold = None

				# print(f"SUCCESS: Data for {month_year_str} found.")

				return {
						'Date': formatted_date,
						'Town': town,
						'Region': region,
						'Median_Sale_Price': median_sale_price,
						'Median_List_Price': median_list_price,
						'Overall_Average_Premium_Paid': overall_average_premium_paid,
						'Median_DOM': median_dom,
						'Avg_Home_Premium': avg_home_premium,
						'Avg_Home_DOM': avg_home_dom,
						'Hot_Home_Premium': hot_home_premium,
						'Hot_Home_DOM': hot_home_dom,
						'Num_of_Homes_Sold': num_homes_sold,
						'Compete_Score': compete_score
				}

		except requests.exceptions.RequestException as e:
				print(f"NETWORK ERROR: Failed to fetch market data for {town}: {e}")
				return None
		except Exception as e:
				print(f"PARSING ERROR in market scraper for {town}: {e}")
				return None

# =========================================================================
# 3. GRIST API COMMUNICATION
# =========================================================================

def push_market_data_to_grist(data_row):
		"""Pushes a new market summary record to the Grist document."""

		# 1. Define the API endpoint for the market data table
		api_url = f"{GRIST_HOST}/api/docs/{GRIST_DOC_ID}/tables/{GRIST_MARKET_TABLE_ID}/records"

		try:
				# 2. Define the HTTP Headers (Using existing keys)
				headers = {
						"Authorization": f"Bearer {GRIST_API_KEY}",
						"Content-Type": "application/json",
						"CF-Access-Client-Id": CF_ACCESS_CLIENT_ID,
						"CF-Access-Client-Secret": CF_ACCESS_CLIENT_SECRET
				}

				# 3. Define the Payload (Matching the scrape_market_summary keys)
				payload = {
						"records": [
								{
										"fields": {
												"Date": data_row['Date'],
												"Town": data_row['Town'],
												"Region": data_row['Region'],
												"Median_Sale_Price": data_row['Median_Sale_Price'],
												"Median_List_Price": data_row['Median_List_Price'],
												"Overall_Average_Premium_Paid": data_row['Overall_Average_Premium_Paid'],
												"Median_DOM": data_row['Median_DOM'],
												"Avg_Home_Premium": data_row['Avg_Home_Premium'],
												"Avg_Home_DOM": data_row['Avg_Home_DOM'],
												"Hot_Home_Premium": data_row['Hot_Home_Premium'],
												"Hot_Home_DOM": data_row['Hot_Home_DOM'],
												"Num_of_Homes_Sold": data_row['Num_of_Homes_Sold'],
												"Compete_Score": data_row['Compete_Score']
										}
								}
						]
				}

				# 4. Make the POST request
				response = requests.post(api_url, headers=headers, json=payload, timeout=15)
				response.raise_for_status() # Raise an exception for bad status codes

				if response.status_code == 200:
						print(f"Successfully added market record for {data_row['Town']} to Grist.")
				else:
						print(f"Failed to push market data to Grist. Status: {response.status_code}. Response: {response.text}")

		except requests.exceptions.RequestException as e:
				print(f"Error communicating with Grist API for market data: {e}")
		except Exception as e:
				print(f"An unexpected error occurred during Grist market API call: {e}")

# =========================================================================
# 4. MAIN EXECUTION
# =========================================================================

if __name__ == "__main__":

		print("\n=========================================================")
		print("--- STARTING MONTHLY MARKET DATA SCRAPER ---")
		print("=========================================================\n")

		all_data_successful = True
		all_redfin_data = [] # List to accumulate all successful Redfin data dictionaries

		# --- PART 1: REDFIN DATA COLLECTION (Existing Loop) ---
		for town, region_url_list in TOWNS_TO_TRACK.items():
				
				region = region_url_list[0]
				url = region_url_list[1]

				market_data = scrape_market_summary(town, region, url)

				if market_data:
						print(f"Ready to push data for {town} (Redfin)")
						all_redfin_data.append(market_data) # Add to the list
				else:
						print(f"Skipping {town} due to data failure.")
						all_data_successful = False # Keep track of failures

		# --- PART 2: STREETEASY NYC DATA COLLECTION (NEW CODE BLOCK) ---
		print("\n=========================================================")
		print("--- STARTING STREETEASY NYC DATA COLLECTION ---")
		print("=========================================================\n")
		
		# Dictionary to map StreetEasy URLs to the final Grist column names
		STREETEASY_METRIC_MAP = {
			'STREETEASY_MEDIAN_RECORDED_PRICE': 'Median_Sale_Price',
			'STREETEASY_MEDIAN_ASKING_PRICE': 'Median_List_Price',
			'STREETEASY_SALE_TO_LIST_RATIO': 'Overall_Average_Premium_Paid',
			'STREETEASY_MEDIAN_DOM': 'Median_DOM',
			'STREETEASY_NUM_OF_HOMES_SOLD': 'Num_of_Homes_Sold'
		}
		
		all_nyc_market_data = {} # Dictionary to hold merged data for NYC towns
		
		# Iterate through each URL/Metric pair
		for url_key, metric_key in STREETEASY_METRIC_MAP.items():
			
			# Use the defined NYC neighborhoods and column indexes
			current_metric_data = scrape_streeteasy_data(
				url_key, 
				metric_key, 
				NYC_NEIGHBORHOODS,
				CSV_COL_INDEXES
			)
			
			# Merge the results from the current metric into the master NYC dictionary
			for town, data in current_metric_data.items():
				if town in all_nyc_market_data:
					all_nyc_market_data[town].update(data)
				else:
					all_nyc_market_data[town] = data
					
		# ----------------------------------------------------------------------
		# 2. CALCULATION STEP: RUN ONCE AFTER ALL MERGING IS COMPLETE
		# ----------------------------------------------------------------------
		RATIO_KEY = 'Overall_Average_Premium_Paid' # The key holding the raw ratio
		
		for town, data in all_nyc_market_data.items():
			
			raw_ratio = data.get(RATIO_KEY)
			
			if raw_ratio is not None:
				try:
					# Convert the string/raw value to a float
					ratio_value = float(raw_ratio)
					premium = ratio_value - 1.0
					
					# Overwrite the column with the calculated premium value
					all_nyc_market_data[town][RATIO_KEY] = premium
					
				except ValueError:
					# Handle cases where the data might be an unparsable string
					all_nyc_market_data[town][RATIO_KEY] = None

		final_nyc_data_list = list(all_nyc_market_data.values())
		print(f"Successfully collected data for {len(final_nyc_data_list)} NYC neighborhoods from StreetEasy.")

		# ==========================================================
		# TEMPORARY CHECK: LOG THE FINAL STREETEASY PAYLOAD
		# ==========================================================
		print("\n--- StreetEasy Payload Check ---")
		for record in final_nyc_data_list:
			# Check for the presence of mandatory keys and a metric key
			if 'Date' in record and 'Town' in record and 'Median_List_Price' in record:
				print(f"SUCCESS: {record['Town']} has Date and Price keys.")
			else:
				print(f"FAILURE: {record.get('Town', 'Unknown Town')} is missing required keys: {record.keys()}")

		# --- PART 3: FINAL PUSH TO GRIST (Revised) ---
		
		# Combine all successful Redfin data and all StreetEasy data
		all_market_data_to_push = all_redfin_data + final_nyc_data_list

		print(f"\nPushing a total of {len(all_market_data_to_push)} records to Grist...")
		
		# Loop through the final list and push each record
		for record in all_market_data_to_push:
			# Normalize the record to ensure all Grist columns are present
			normalized_record = normalize_record_for_grist(record, GRIST_MASTER_SCHEMA)
			
			# The 'push_market_data_to_grist' function should now use the normalized record
			push_market_data_to_grist(normalized_record) 
			
		print("\n--- Market Data Script Finished ---")