import json
import logging
import sys
from dataclasses import dataclass
from typing import Any, Callable

import requests


def main():
    settings = Settings(
        endpoint="https://esg-ai477291929312.cognitiveservices.azure.com",
        api_version="2024-12-01-preview",
        subscription_key="1c094f97ba6d4679abdc37e918911b39",
        analyzer_id="multi-period-analyzer",  # New analyzer ID
    )
    
    client = AzureContentUnderstandingClient(
        settings.endpoint,
        settings.api_version,
        subscription_key=settings.subscription_key,
        token_provider=settings.token_provider,
    )
    
    # Load the multi-period analyzer definition
    with open("multi_period_request.json", "r") as f:
        analyzer_definition = json.load(f)
    
    # First, delete the analyzer if it exists
    try:
        print(f"Checking if analyzer '{settings.analyzer_id}' exists and deleting it...")
        response = client.delete_analyzer(settings.analyzer_id)
        # Delete is synchronous and returns 204 No Content on success
        print(f"Delete operation completed successfully. Status code: {response.status_code}")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Analyzer '{settings.analyzer_id}' doesn't exist yet. Proceeding to create it.")
        else:
            print(f"Error deleting analyzer: {e}")
            print(f"Response status code: {e.response.status_code}")
            print(f"Response text: {e.response.text}")
            print("Continuing with create operation anyway...")
    
    # Make sure field schema has a name (required by API)
    if "fieldSchema" in analyzer_definition and "name" not in analyzer_definition["fieldSchema"]:
        analyzer_definition["fieldSchema"]["name"] = "UtilityBillSchema"
    
    print("\nCreating analyzer with the following configuration:")
    print(json.dumps(analyzer_definition, indent=2))
    
    # Create the analyzer
    try:
        response = client.create_analyzer(settings.analyzer_id, analyzer_definition)
        print(f"Multi-period analyzer creation started. Operation URL: {response.headers.get('operation-location')}")
        
        # Wait for analyzer creation to complete
        result = client.poll_result(
            response,
            timeout_seconds=60 * 5,
            polling_interval_seconds=2,
        )
        
        print("Multi-period analyzer creation result:")
        json.dump(result, sys.stdout, indent=2)
        
        if result.get("status") == "ready":
            print("\nAnalyzer created successfully and is ready for use!")
        else:
            print(f"\nAnalyzer creation completed with status: {result.get('status')}")
            
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error creating analyzer: {e}")
        print(f"Response status code: {e.response.status_code}")
        print(f"Response text: {e.response.text}")
        try:
            error_details = e.response.json()
            print("Error details:")
            json.dump(error_details, sys.stdout, indent=2)
        except:
            print("Could not parse error response as JSON")


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

    def create_analyzer(self, analyzer_id: str, analyzer_definition: dict[str, Any]):
        """
        Creates a new analyzer with the specified ID and definition.

        Args:
            analyzer_id (str): The ID to assign to the new analyzer.
            analyzer_definition (dict): The definition of the analyzer.

        Returns:
            Response: The response from the create analyzer request.

        Raises:
            HTTPError: If the HTTP request returned an unsuccessful status code.
        """
        headers = {"Content-Type": "application/json"}
        headers.update(self._headers)

        url = f"{self._endpoint}/contentunderstanding/analyzers/{analyzer_id}?api-version={self._api_version}"
        self._logger.info(f"Creating analyzer: {analyzer_id}")
        
        response = requests.put(
            url=url,
            headers=headers,
            json=analyzer_definition,
        )

        response.raise_for_status()
        return response
        
    def delete_analyzer(self, analyzer_id: str):
        """
        Deletes an existing analyzer with the specified ID.
        This is a synchronous operation that returns a 204 No Content status code on success.

        Args:
            analyzer_id (str): The ID of the analyzer to delete.

        Returns:
            Response: The response from the delete analyzer request.

        Raises:
            HTTPError: If the HTTP request returned an unsuccessful status code.
        """
        url = f"{self._endpoint}/contentunderstanding/analyzers/{analyzer_id}?api-version={self._api_version}"
        self._logger.info(f"Deleting analyzer: {analyzer_id}")
        
        response = requests.delete(
            url=url,
            headers=self._headers,
        )

        response.raise_for_status()
        return response

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

        headers = {"Content-Type": "application/json"}
        headers.update(self._headers)

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
            if status == "succeeded" or status == "ready":
                self._logger.info(
                    f"Request result is ready after {elapsed_time:.2f} seconds."
                )
                return result
            elif status == "failed":
                self._logger.error(f"Request failed. Reason: {response.json()}")
                raise RuntimeError("Request failed.")
            else:
                self._logger.info(
                    f"Request {operation_location.split('/')[-1].split('?')[0]} in progress ..."
                )
            time.sleep(polling_interval_seconds)


if __name__ == "__main__":
    main() 