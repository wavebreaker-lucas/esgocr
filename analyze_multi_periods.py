import json
import logging
import sys
import os
from dataclasses import dataclass
from typing import Any, Callable, List, Dict
from pathlib import Path

import requests


def main():
    # Sample URL to analyze - replace with your utility bill URL
    file_url = "https://raw.githubusercontent.com/wavebreaker-lucas/esgocr/main/utility_bills/HKE1.png"
    
    # Output directory for results
    results_dir = "analysis_results"
    os.makedirs(results_dir, exist_ok=True)
    
    settings = Settings(
        endpoint="https://esg-ai477291929312.cognitiveservices.azure.com",
        api_version="2024-12-01-preview",
        subscription_key="1c094f97ba6d4679abdc37e918911b39",
        analyzer_id="multi-period-analyzer",  # Using the multi-period analyzer
    )
    
    client = AzureContentUnderstandingClient(
        settings.endpoint,
        settings.api_version,
        subscription_key=settings.subscription_key,
        token_provider=settings.token_provider,
    )
    
    # Get filename from URL for output naming
    file_basename = os.path.basename(file_url)
    file_name = os.path.splitext(file_basename)[0]
    
    # Analyze the document
    response = client.begin_analyze(settings.analyzer_id, file_url)
    print(f"Analysis started. Operation URL: {response.headers.get('operation-location')}")
    
    # Wait for analysis to complete
    result = client.poll_result(
        response,
        timeout_seconds=60 * 5,
        polling_interval_seconds=2,
    )
    
    # Save complete raw results
    raw_results_path = os.path.join(results_dir, f"{file_name}_raw_results.json")
    with open(raw_results_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"\nComplete raw results saved to: {raw_results_path}")
    
    # Extract and display the extracted fields
    if result.get("status") == "Succeeded" and "result" in result:
        try:
            fields = result["result"]["contents"][0]["fields"]
            print("\nExtracted Information:")
            
            # Extract standard single fields
            billing_period = fields.get("BillingPeriod", {}).get("valueString", "Not found")
            consumption = fields.get("ElectricityConsumption", {}).get("valueNumber", "Not found")
            
            print(f"Primary Billing Period: {billing_period}")
            print(f"Primary Electricity Consumption: {consumption} kWh")
            
            # Extract multiple billing periods from the generated field
            billing_data = []
            
            if "MultipleBillingPeriods" in fields and "valueString" in fields["MultipleBillingPeriods"]:
                multiple_periods_str = fields["MultipleBillingPeriods"]["valueString"]
                
                # Try to parse as JSON
                try:
                    multiple_periods = json.loads(multiple_periods_str)
                    if isinstance(multiple_periods, list) and multiple_periods:
                        print(f"\nFound {len(multiple_periods)} billing periods:")
                        
                        standardized_data = []
                        for i, period in enumerate(multiple_periods, 1):
                            if isinstance(period, dict):
                                # Convert to our standard format
                                standardized_period = {
                                    "period": period.get("period", period.get("請表日期", "Unknown")),
                                    "consumption": parse_consumption(period.get("consumption", period.get("用電度數", "0")))
                                }
                                
                                standardized_data.append(standardized_period)
                                
                                print(f"\nPeriod {i}:")
                                print(f"  Date: {standardized_period['period']}")
                                print(f"  Consumption: {standardized_period['consumption']} kWh")
                                
                        # Save standardized data
                        if standardized_data:
                            standardized_path = os.path.join(results_dir, f"{file_name}_periods.json")
                            with open(standardized_path, "w", encoding="utf-8") as f:
                                json.dump(standardized_data, f, indent=2, ensure_ascii=False)
                            print(f"\nStandardized billing data saved to: {standardized_path}")
                    else:
                        print("\nNo billing periods found in the extracted data.")
                        
                except json.JSONDecodeError:
                    print("\nCould not parse multiple billing periods as JSON. Raw output:")
                    print(multiple_periods_str)
            else:
                print("\nNo multiple billing periods found")
                
        except (KeyError, IndexError) as e:
            print(f"\nError parsing results: {e}")
            print("Could not find the expected fields in the response.")


def parse_consumption(consumption_str):
    """Convert various consumption string formats to a numeric value"""
    if isinstance(consumption_str, (int, float)):
        return consumption_str
    
    # Remove thousand separators and other non-numeric characters except decimal point
    if isinstance(consumption_str, str):
        # Remove commas, spaces, and other separators
        numeric_str = consumption_str.replace(',', '').replace(' ', '')
        
        # Try to extract just the numeric part if there are other characters
        import re
        numeric_match = re.search(r'[-+]?\d*\.?\d+', numeric_str)
        if numeric_match:
            numeric_str = numeric_match.group(0)
            
        try:
            return float(numeric_str)
        except ValueError:
            pass
            
    return 0  # Default if parsing fails


@dataclass(frozen=True, kw_only=True)
class Settings:
    endpoint: str
    api_version: str
    subscription_key: str | None = None
    aad_token: str | None = None
    analyzer_id: str

    def __post_init__(self):
        key_not_provided = (
            not self.subscription_key
            or self.subscription_key == "AZURE_CONTENT_UNDERSTANDING_SUBSCRIPTION_KEY"
        )
        token_not_provided = (
            not self.aad_token
            or self.aad_token == "AZURE_CONTENT_UNDERSTANDING_AAD_TOKEN"
        )
        if key_not_provided and token_not_provided:
            raise ValueError(
                "Either 'subscription_key' or 'aad_token' must be provided"
            )

    @property
    def token_provider(self) -> Callable[[], str] | None:
        aad_token = self.aad_token
        if aad_token is None:
            return None

        return lambda: aad_token


class AzureContentUnderstandingClient:
    def __init__(
        self,
        endpoint: str,
        api_version: str,
        subscription_key: str | None = None,
        token_provider: Callable[[], str] | None = None,
        x_ms_useragent: str = "cu-sample-code",
    ) -> None:
        if not subscription_key and token_provider is None:
            raise ValueError(
                "Either subscription key or token provider must be provided"
            )
        if not api_version:
            raise ValueError("API version must be provided")
        if not endpoint:
            raise ValueError("Endpoint must be provided")

        self._endpoint: str = endpoint.rstrip("/")
        self._api_version: str = api_version
        self._logger: logging.Logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)
        self._headers: dict[str, str] = self._get_headers(
            subscription_key, token_provider and token_provider(), x_ms_useragent
        )

    def begin_analyze(self, analyzer_id: str, file_location: str):
        """
        Begins the analysis of a file or URL using the specified analyzer.

        Args:
            analyzer_id (str): The ID of the analyzer to use.
            file_location (str): The path to the file or the URL to analyze.

        Returns:
            Response: The response from the analysis request.

        Raises:
            ValueError: If the file location is not a valid path or URL.
            HTTPError: If the HTTP request returned an unsuccessful status code.
        """
        import os
        from pathlib import Path
        
        if Path(file_location).exists():
            with open(file_location, "rb") as file:
                data = file.read()
            headers = {"Content-Type": "application/octet-stream"}
        elif "https://" in file_location or "http://" in file_location:
            data = {"url": file_location}
            headers = {"Content-Type": "application/json"}
        else:
            raise ValueError("File location must be a valid path or URL.")

        headers.update(self._headers)
        url = self._get_analyze_url(self._endpoint, self._api_version, analyzer_id)
        
        self._logger.info(f"Analyzing file {file_location} with analyzer: {analyzer_id}")
        self._logger.info(f"POST request to: {url}")
        
        if isinstance(data, dict):
            response = requests.post(
                url=url,
                headers=headers,
                json=data,
            )
        else:
            response = requests.post(
                url=url,
                headers=headers,
                data=data,
            )

        response.raise_for_status()
        return response

    def poll_result(
        self,
        response: requests.Response,
        timeout_seconds: int = 120,
        polling_interval_seconds: int = 2,
    ) -> dict[str, Any]:
        """
        Polls the result of an asynchronous operation until it completes or times out.
        """
        operation_location = response.headers.get("operation-location", "")
        if not operation_location:
            raise ValueError("Operation location not found in response headers.")

        import time
        start_time = time.time()
        while True:
            elapsed_time = time.time() - start_time
            self._logger.info(
                f"Waiting for service response (elapsed: {elapsed_time:.2f}s)"
            )
            if elapsed_time > timeout_seconds:
                raise TimeoutError(
                    f"Operation timed out after {timeout_seconds:.2f} seconds."
                )

            response = requests.get(operation_location, headers=self._headers)
            response.raise_for_status()
            result = response.json()
            status = result.get("status", "").lower()
            if status == "succeeded":
                self._logger.info(
                    f"Analysis completed after {elapsed_time:.2f} seconds."
                )
                return result
            elif status == "failed":
                self._logger.error(f"Analysis failed. Reason: {response.json()}")
                raise RuntimeError(f"Analysis failed: {result}")
            else:
                self._logger.info(
                    f"Analysis in progress... (elapsed: {elapsed_time:.2f}s)"
                )
            time.sleep(polling_interval_seconds)

    def _get_analyze_url(self, endpoint: str, api_version: str, analyzer_id: str):
        return f"{endpoint}/contentunderstanding/analyzers/{analyzer_id}:analyze?api-version={api_version}"

    def _get_headers(
        self, subscription_key: str | None, api_token: str | None, x_ms_useragent: str
    ) -> dict[str, str]:
        """Returns the headers for the HTTP requests."""
        headers = (
            {"Ocp-Apim-Subscription-Key": subscription_key}
            if subscription_key
            else {"Authorization": f"Bearer {api_token}"}
        )
        headers["x-ms-useragent"] = x_ms_useragent
        return headers


if __name__ == "__main__":
    main() 