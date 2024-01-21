import pandas as pd
import numpy as np

# Read

data = pd.read_csv('NIFTY 50.csv')
# data = pd.read_csv('NIFTY 50-6m.csv')  # <- Data Downloaded from the nseindia website

data.columns = data.columns.str.strip()


# Data Engineering
data['Daily Returns'] = data['Close'].pct_change()


# Calculate Daily & Annualized Volatility 

daily_volatility = np.std(data['Daily Returns'])
annualized_volatility = daily_volatility * np.sqrt(len(data))

data.to_csv('output-data.csv', index=False) 

print('The Data Has Been Processed Successfully ...')
print('____________________________________________')
print("Daily Returns: \t\t Saved in [ output-data.csv ] file.")
print("Daily Volatility: \t", daily_volatility)
print("Annualized Volatility: \t", annualized_volatility)
