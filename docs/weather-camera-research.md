# Weather Camera Research

## Canonical Weather Data

V3 should treat `api.weather.gov` as the canonical live weather reference and the final NWS Daily Climate Report as the settlement truth. Camera feeds are optional secondary features for regime detection and predictive image analysis.

## Camera Network Shortlist

### WeatherBug Cameras
- URL: `https://www.weatherbug.com/cameras`
- Why it matters:
  - large U.S. weather-camera directory
  - city and regional pages are already organized around weather use cases
  - good fit for metro weather markets like Chicago, New York City, Los Angeles, Miami, Denver, and Dallas
- Risks:
  - commercial site, unclear programmatic terms for large-scale scraping
  - camera availability varies by city

### Windy Webcams API
- URL: `https://api.windy.com/webcams`
- Why it matters:
  - structured API instead of pure page scraping
  - broad global webcam inventory
  - stronger fit if we want a provider abstraction with API keys and metadata-rich search
- Risks:
  - paid tier for meaningful scale
  - webcam quality and update cadence vary

### AirportWebcams.net
- URL: `https://airportwebcams.net/`
- Why it matters:
  - airport-heavy coverage lines up well with many Kalshi weather cities
  - good overlap for Chicago, New York City, Los Angeles, Miami, Denver, Seattle, Atlanta, Dallas, Minneapolis
  - airport views are often useful for cloud cover, visibility, precipitation, and surface-state context
- Risks:
  - aggregation site, not a formal machine-friendly API
  - airport microclimate may not exactly match settlement station

### State DOT / 511 Camera Networks
- Example family:
  - `511.org`
  - state DOT traffic camera sites
- Why it matters:
  - official public-sector feeds in many metro areas
  - strong coverage for traffic-heavy Kalshi weather cities
  - often easier to justify as observational context than tourism webcams
- Risks:
  - fragmented across states
  - inconsistent formats, uptime, and access controls

## Recommended Ranking

1. `WeatherBug` for broad U.S. weather-camera coverage.
2. `Windy Webcams API` if we want a cleaner provider/API integration.
3. `AirportWebcams.net` for airport-centric metro coverage.
4. `DOT / 511 cameras` as city-specific fallback where commercial feeds are weak.

## V3 Recommendation

- Keep cameras behind a disabled experimental adapter.
- Never let camera output become the settlement truth.
- Use them only for:
  - cloud cover / solar heating regime
  - precipitation / wet-ground / snow persistence context
  - visibility / fog conditions
  - coarse daytime-nowcast support

- First camera rollouts should target the cities with the best likely overlap:
  - Chicago
  - New York City
  - Los Angeles
  - Miami
  - Denver
  - Seattle
  - Atlanta
  - Dallas
  - Minneapolis
