# TcgCsvProvider Documentation

## Overview

The `TcgCsvProvider` is a MTGJSON provider for fetching pricing data from tcgcsv.com public API endpoints. This provider enriches MTGJSON card data with additional pricing information from TCGCSV's comprehensive database.

## API Integration

### Base Configuration

- **Base URL**: `https://tcgcsv.com/tcgplayer/1`
- **Authentication**: None required (public endpoints)
- **Rate Limiting**: Managed by AbstractProvider base class
- **Caching**: Enabled via AbstractProvider session

### Endpoints

#### Pricing Data Endpoint

```
GET /{groupId}/prices
```

**Description**: Fetches pricing data for all products in a specific set/group.

**Parameters**:
- `groupId` (string): TCGCSV group identifier for the Magic set

**Example Request**:
```
GET https://tcgcsv.com/tcgplayer/1/123/prices
```

**Response Schema**:
```json
{
  "success": true,
  "errors": [],
  "results": [
    {
      "productId": 1001,
      "lowPrice": 8.00,
      "midPrice": 9.25, 
      "highPrice": 12.00,
      "marketPrice": 10.50,
      "directLowPrice": 9.50,
      "subTypeName": "Normal"
    },
    {
      "productId": 1002,
      "marketPrice": 25.75,
      "subTypeName": "Foil"
    }
  ]
}
```

**Response Fields**:
- `success` (boolean): Whether the API call succeeded
- `errors` (array): List of error messages if success=false
- `results` (array): Array of price data objects
  - `productId` (integer): TCGCSV product identifier
  - `lowPrice` (float, optional): Low market price
  - `midPrice` (float, optional): Mid market price  
  - `highPrice` (float, optional): High market price
  - `marketPrice` (float, optional): Current market price
  - `directLowPrice` (float, optional): Direct low price
  - `subTypeName` (string): Card finish type ("Normal", "Foil", "Etched", etc.)

## Provider Implementation

### Class Structure

```python
@singleton
class TcgCsvProvider(AbstractProvider):
    """TCGCSV provider for pricing data"""
    
    base_url: str = "https://tcgcsv.com/tcgplayer/1"
```

### Key Methods

#### `fetch_set_prices(set_code: str, group_id: str) -> List[Dict[str, Any]]`

Fetches raw pricing data for a specific set from TCGCSV API.

**Parameters**:
- `set_code`: MTGJSON set code (for logging)
- `group_id`: TCGCSV group ID

**Returns**: List of price data dictionaries

**Error Handling**: Returns empty list on API errors or network issues

#### `convert_to_mtgjson_prices(price_data: List[Dict], set_code: str) -> Dict[str, MtgjsonPricesObject]`

Converts TCGCSV price data to MTGJSON price objects.

**Data Mapping**:
- `productId` → Used as temporary key (needs UUID mapping)
- `marketPrice` → `sell_normal`, `sell_foil`, or `sell_etched` based on `subTypeName`
- `subTypeName` → Determines price field assignment

**Price Field Logic**:
```python
if "etched" in sub_type_name.lower():
    price_obj.sell_etched = market_price
elif "foil" in sub_type_name.lower():
    price_obj.sell_foil = market_price
else:
    price_obj.sell_normal = market_price
```

#### `generate_today_price_dict_for_set(set_code: str, group_id: str) -> Dict[str, MtgjsonPricesObject]`

Main entry point for fetching and converting price data for a set.

**Workflow**:
1. Fetch raw price data via `fetch_set_prices()`
2. Convert to MTGJSON format via `convert_to_mtgjson_prices()`
3. Return price dictionary

## Integration Points

### Current Implementation Status

✅ **Completed**:
- Basic provider class with AbstractProvider inheritance
- HTTP session management and error handling
- TCGCSV API integration for pricing endpoints
- Price data parsing and conversion to MtgjsonPricesObject
- Unit tests with 100% coverage
- Error handling for network issues and API failures

⚠️ **Pending** (out of scope for TCG-78):
- Collector number mapping between TCGCSV productId and MTGJSON UUID
- Integration with price_builder.py
- Real group ID resolution from MTGJSON set metadata

### Future Integration Requirements

1. **Group ID Resolution**: Need mapping from MTGJSON set codes to TCGCSV group IDs
2. **Product ID Mapping**: Need mapping from TCGCSV productId to MTGJSON card UUIDs
3. **Price Builder Integration**: Register provider in `mtgjson5/price_builder.py`
4. **Collector Number Matching**: Map cards by collector number for accurate pricing

## Example Usage

```python
from mtgjson5.providers.tcgcsv_provider import TcgCsvProvider

# Initialize provider
provider = TcgCsvProvider()

# Fetch prices for a set (requires real group ID)
prices = provider.generate_today_price_dict_for_set("FIC", "12345")

# Example price object
price_obj = prices["1001"]  # productId key
print(f"Normal: ${price_obj.sell_normal}")
print(f"Foil: ${price_obj.sell_foil}")  
print(f"Provider: {price_obj.provider}")  # "tcgcsv"
```

## Testing

Unit tests are located in `tests/mtgjson5/providers/test_tcgcsv_provider.py`.

**Test Coverage**:
- HTTP header construction
- API success and failure scenarios
- Price data conversion logic
- Error handling for malformed data
- Network exception handling

**Run Tests**:
```bash
python -m pytest tests/mtgjson5/providers/test_tcgcsv_provider.py -v
```

## Error Scenarios

### API Errors
- **403 Forbidden**: Invalid group ID or access restrictions
- **404 Not Found**: Group ID doesn't exist
- **500 Server Error**: TCGCSV API issues

### Data Issues
- **success=false**: API returns error response
- **Missing productId**: Price records without product identifier
- **Invalid prices**: Non-numeric price values

### Network Issues
- **Connection timeout**: Network connectivity problems
- **DNS resolution**: tcgcsv.com unavailable

All errors are logged and gracefully handled by returning empty results.

## Related Files

- **Provider**: `mtgjson5/providers/tcgcsv_provider.py`
- **Tests**: `tests/mtgjson5/providers/test_tcgcsv_provider.py`  
- **Demo**: `demo_tcgcsv_provider.py`
- **Base Class**: `mtgjson5/providers/abstract.py`
- **Price Objects**: `mtgjson5/classes/mtgjson_prices.py`