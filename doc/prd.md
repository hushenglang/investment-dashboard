## Business Requirement
- i want to build web application "investment dashboard" containing frontend and backend.
- backend is using python. 
    - there is scheduler component which is to fetch and store economic indicators.
    - there is REST API component which is to server frontend call for display indicators.
- frontend is using react.js

## Non-Function Requirement
- backend tech stack
    - FastAPI
- frontend tech stack
    - react.js

## Economic Indicators
### US Economic Indicators
| Category | Indicator | Why It Matters |
|----------|-----------|----------------|
| Leading Economic Index | US Leading Index (USALOLITOAASTSAM) | Predicts future economic trends and potential turning points in the economy |
| Consumer Metrics | Total Consumer Credit (TOTALSL) | Shows consumer borrowing trends and financial health |
| Consumer Metrics | University of Michigan Consumer Sentiment (UMCSENT) | Measures consumer confidence and potential future spending |
| Consumer Metrics | Real Disposable Personal Income (DSPIC96) | Indicates consumers' purchasing power and potential spending |
| Financial Conditions | Chicago Fed National Financial Conditions Index (NFCI) | Measures overall financial system stability |
| Financial Conditions | Chicago Fed Adjusted National Financial Conditions Index (ANFCI) | Risk-adjusted measure of financial system stability |
| Manufacturing & Services | ISM Manufacturing PMI | Indicates expansion/contraction in manufacturing sector (>50 expansion, <50 contraction) |
| Manufacturing & Services | ISM Services PMI | Shows growth/decline in services sector |
| Manufacturing & Services | Composite PMI | Combined measure of both manufacturing and services sector performance |
| Treasury Yields | 3-Month Treasury Yield (DGS3MO) | Short-term government borrowing cost |
| Treasury Yields | 2-Year Treasury Yield (DGS2) | Medium-term interest rate expectations |
| Treasury Yields | 10-Year Treasury Yield (DGS10) | Long-term interest rate benchmark |
| Yield Spreads | 10Y-2Y Spread | Key recession indicator (negative spread often precedes recessions) |
| Yield Spreads | 10Y-3M Spread | Alternative recession indicator with shorter-term comparison |

### China Economic Indicators
| **Category**          | **Indicator**                          | **Why It Matters**                                                                 |
|-----------------------|----------------------------------------|-----------------------------------------------------------------------------------|
| **GDP Growth**         | Quarterly GDP Growth Rate              | Measures overall economic expansion or contraction.                               |
| **Industrial Output**  | Industrial Production Growth           | Reflects manufacturing and industrial sector health.                              |
| **Retail Sales**       | Retail Sales Growth (YoY)              | Indicates consumer demand and domestic consumption trends.                        |
| **Fixed Investment**   | Fixed Asset Investment (FAI) Growth    | Shows capital spending by businesses and government (key for infrastructure).     |
| **Trade**             | Exports & Imports (Trade Balance)      | Reveals external demand (exports) and domestic demand for foreign goods (imports).|
| **Inflation**         | Consumer Price Index (CPI)             | Tracks price changes for consumer goods (indicates inflation/deflation).          |
| **Producer Prices**   | Producer Price Index (PPI)             | Measures wholesale price trends (signals future CPI movements).                   |
| **Employment**        | Surveyed Urban Unemployment Rate       | Indicates labor market health (official & unofficial measures differ).            |
| **Manufacturing**     | PMI (Purchasing Managers' Index)       | Leading indicator for manufacturing sector expansion (>50) or contraction (<50).  |
| **Non-Manufacturing** | Non-Manufacturing PMI                  | Tracks services and construction sector health.                                   |
| **Credit Growth**     | Total Social Financing (TSF)           | Measures overall credit and liquidity in the economy (key for stimulus tracking). |
| **Money Supply**      | M2 Money Supply Growth                | Indicates monetary policy stance (loose or tight).                                |
| **Interest Rates**    | Loan Prime Rate (LPR)                  | PBOC's benchmark lending rates (affects borrowing costs).                         |
| **Housing Market**    | New Home Prices & Property Investment  | Reflects real estate sector health (critical for China's economy).                |
| **Foreign Reserves**  | Foreign Exchange Reserves              | Shows China's ability to manage currency and external shocks.                     |

### reference
- AKShare Indicators URL: https://github.com/akfamily/akshare/blob/09fe6e81d23952c1bf826b5c0daf4050e0b7089a/docs/tutorial.md 