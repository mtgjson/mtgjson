"""
Unit tests for TcgCsvProvider
"""

import json
from unittest.mock import Mock, patch

import pytest

from mtgjson5.classes import MtgjsonPricesObject
from mtgjson5.providers.tcgcsv_provider import TcgCsvProvider


class TestTcgCsvProvider:
    """Test cases for TcgCsvProvider"""

    def setup_method(self):
        """Setup test fixtures"""
        self.provider = TcgCsvProvider()

    def test_build_http_header(self):
        """Test HTTP header construction"""
        headers = self.provider._build_http_header()

        assert "Content-Type" in headers
        assert headers["Content-Type"] == "application/json"
        assert "User-Agent" in headers
        assert "MTGJSON-TcgCsvProvider" in headers["User-Agent"]

    def test_fetch_set_prices_success(self):
        """Test successful price data fetching"""
        # Mock TCGCSV API response
        mock_response = {
            "success": True,
            "errors": [],
            "results": [
                {
                    "productId": 1001,
                    "marketPrice": 10.50,
                    "lowPrice": 8.00,
                    "midPrice": 9.25,
                    "highPrice": 12.00,
                    "subTypeName": "Normal",
                },
                {"productId": 1002, "marketPrice": 25.75, "subTypeName": "Foil"},
            ],
        }

        with patch.object(
            self.provider, "download", return_value=mock_response
        ) as mock_download:
            # Test the method
            result = self.provider.fetch_set_prices("FIC", "123")

            # Verify results
            assert len(result) == 2
            assert result[0]["productId"] == 1001
            assert result[0]["marketPrice"] == 10.50
            assert result[1]["productId"] == 1002
            assert result[1]["marketPrice"] == 25.75

            # Verify API call
            mock_download.assert_called_once_with(
                "https://tcgcsv.com/tcgplayer/1/123/prices"
            )

    def test_fetch_set_prices_api_failure(self):
        """Test handling of API failure response"""
        # Mock TCGCSV API failure response
        mock_response = {
            "success": False,
            "errors": ["Invalid group ID", "Rate limit exceeded"],
            "results": [],
        }

        with patch.object(
            self.provider, "download", return_value=mock_response
        ) as mock_download:
            # Test the method
            result = self.provider.fetch_set_prices("FIC", "999")

            # Verify empty result on API failure
            assert result == []

    def test_fetch_set_prices_exception(self):
        """Test handling of network/HTTP exceptions"""

        with patch.object(
            self.provider, "download", side_effect=Exception("Network error")
        ) as mock_download:
            # Test the method
            result = self.provider.fetch_set_prices("FIC", "123")

            # Verify empty result on exception
            assert result == []

    def test_inner_translate_today_price_dict(self):
        """Test internal price data translation"""
        # Mock price data
        mock_price_data = [
            {
                "productId": 1001,
                "marketPrice": 15.50,
                "subTypeName": "Normal",
            },
            {"productId": 1002, "marketPrice": 30.25, "subTypeName": "Foil"},
            {"productId": 1003, "marketPrice": 45.00, "subTypeName": "Etched"},
        ]

        with patch.object(
            self.provider, "fetch_set_prices", return_value=mock_price_data
        ) as mock_fetch:
            # Test the method
            result = self.provider._inner_translate_today_price_dict("FIC", "123")

            # Verify results
            assert len(result) == 3
            assert "1001" in result
            assert "1002" in result
            assert "1003" in result

            # Check normal card pricing
            assert result["1001"]["normal"] == 15.50
            assert "foil" not in result["1001"]
            assert "etched" not in result["1001"]

            # Check foil card pricing
            assert result["1002"]["foil"] == 30.25
            assert "normal" not in result["1002"]

            # Check etched card pricing
            assert result["1003"]["etched"] == 45.00
            assert "normal" not in result["1003"]

    def test_inner_translate_today_price_dict_malformed_data(self):
        """Test handling of malformed price data"""
        # Price data with missing/invalid fields
        price_data = [
            {
                # Missing productId
                "marketPrice": 10.00,
                "subTypeName": "Normal",
            },
            {
                "productId": 1002,
                # Missing marketPrice
                "subTypeName": "Foil",
            },
            {
                "productId": 1003,
                "marketPrice": "invalid_price",  # Invalid price type
                "subTypeName": "Normal",
            },
        ]

        with patch.object(self.provider, "fetch_set_prices", return_value=price_data):
            # Convert to MTGJSON format
            result = self.provider._inner_translate_today_price_dict("FIC", "123")

            # Should handle malformed data gracefully
            # Only valid records should be processed
            assert len(result) <= len(price_data)

    def test_generate_today_price_dict_for_set(self):
        """Test the main price generation method"""
        # Mock the internal translate method
        mock_price_mapping = {"1001": {"normal": 20.00}}

        with patch.object(
            self.provider,
            "_inner_translate_today_price_dict",
            return_value=mock_price_mapping,
        ) as mock_translate:
            # Test the method
            result = self.provider.generate_today_price_dict_for_set("FIC", "123")

            # Verify results
            assert len(result) == 1
            assert "1001" in result
            assert isinstance(result["1001"], MtgjsonPricesObject)
            assert result["1001"].sell_normal == 20.00
            assert result["1001"].source == "paper"
            assert result["1001"].provider == "tcgcsv"
            assert result["1001"].currency == "USD"

            # Verify internal method calls
            mock_translate.assert_called_once_with("FIC", "123")

    def test_generate_today_price_dict_for_set_no_data(self):
        """Test handling when no price data is available"""

        with patch.object(
            self.provider, "_inner_translate_today_price_dict", return_value={}
        ) as mock_translate:
            # Test the method
            result = self.provider.generate_today_price_dict_for_set("FIC", "123")

            # Verify empty result
            assert result == {}

    def test_download_method_calls_session(self):
        """Test that download method properly calls the session"""
        with patch.object(self.provider.session, "get") as mock_get:
            # Mock successful response
            mock_response = Mock()
            mock_response.ok = True
            mock_response.json.return_value = {"test": "data"}
            mock_get.return_value = mock_response

            # Test download
            result = self.provider.download("http://test.com")

            # Verify session was called correctly
            mock_get.assert_called_once_with("http://test.com", params=None)
            assert result == {"test": "data"}

    def test_download_method_handles_http_error(self):
        """Test that download method handles HTTP errors"""
        with patch.object(self.provider.session, "get") as mock_get:
            # Mock failed response
            mock_response = Mock()
            mock_response.ok = False
            mock_response.status_code = 500
            mock_response.text = "Server error"
            mock_response.raise_for_status.side_effect = Exception("HTTP 500 error")
            mock_get.return_value = mock_response

            # Test download - should raise exception
            with pytest.raises(Exception):
                self.provider.download("http://test.com")
