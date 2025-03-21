import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List

import requests


def main():
    # Directory containing utility bills
    bills_directory = "utility_bills"
    results_directory = "analysis_results"
    
    # Create results directory if it doesn't exist
    os.makedirs(results_directory, exist_ok=True)
    
    settings = Settings(
        endpoint="https://esg-ai477291929312.cognitiveservices.azure.com",
        api_version="2024-12-01-preview",
        subscription_key="1c094f97ba6d4679abdc37e918911b39",
        analyzer_id="utility-bill-analyzer",
    )
    
    client = AzureContentUnderstandingClient(
        settings.endpoint,
        settings.api_version,
        subscription_key=settings.subscription_key,
        token_provider=settings.token_provider,
    )
    
    # Get all files in the directory
    bill_files = list(Path(bills_directory).glob("*.*"))
    
    if not bill_files:
        print(f"No files found in {bills_directory}. Please add utility bill files to this directory.")
        return
    
    # Process each file
    results = []
    for bill_file in bill_files:
        print(f"\nProcessing: {bill_file}")
        try:
            # Analyze the document
            response = client.begin_analyze(settings.analyzer_id, str(bill_file))
            print(f"Analysis started. Operation URL: {response.headers.get('operation-location')}")
            
            # Wait for analysis to complete
            result = client.poll_result(
                response,
                timeout_seconds=60 * 5,
                polling_interval_seconds=2,
            )
            
            # Extract fields
            if result.get("status") == "Succeeded" and "result" in result:
                try:
                    fields = result["result"]["contents"][0]["fields"]
                    extracted_data = {
                        "filename": bill_file.name,
                        "billing_period": fields.get("BillingPeriod", {}).get("valueString", "Not found"),
                        "electricity_consumption": fields.get("ElectricityConsumption", {}).get("valueNumber", "Not found"),
                    }
                    results.append(extracted_data)
                    
                    # Save individual result
                    result_file = Path(results_directory) / f"{bill_file.stem}_analysis.json"
                    with open(result_file, "w") as f:
                        json.dump(result, f, indent=2)
                    
                    print(f"Extracted Information for {bill_file.name}:")
                    print(f"  Billing Period: {extracted_data['billing_period']}")
                    print(f"  Electricity Consumption: {extracted_data['electricity_consumption']} kWh")
                    print(f"  Full results saved to: {result_file}")
                    
                except (KeyError, IndexError) as e:
                    print(f"Could not extract fields from {bill_file.name}: {e}")
            else:
                print(f"Analysis failed for {bill_file.name}: {result.get('status')}")
                
        except Exception as e:
            print(f"Error processing {bill_file.name}: {e}")
    
    # Save summary of all results
    if results:
        summary_file = Path(results_directory) / "analysis_summary.json"
        with open(summary_file, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nSummary of all analyses saved to: {summary_file}")


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