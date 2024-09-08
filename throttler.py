from dataclasses import InitVar, dataclass, field
import datetime
import math
from operator import is_
from pprint import pprint
import time
from collections import deque
import random
import requests

@dataclass
class _RequestThrottlerDefaultsBase:
    """Default values for the RequestThrottler class."""
    max_requests_in_window: int = 10  # requests per window
    rate_limit_window: int = 1  # in seconds
    throttle_start_percentage: float = 0.75  # Start throttling at 75% of the limit
    full_throttle_percentage: float = 0.90  # Fully throttle at 90% of the limit
    full_throttle_buffer: float = 0.10  # Add a cushion to the full throttle limit
    backoff_retries: int = 3  # Number of retries for exponential backoff
    backoff_base_delay: float = 2.0  # Base delay for exponential backoff
    backoff_factor: float = 2.0  # Exponential backoff factor
    backoff_max_delay: float = 3600.0  # Maximum delay for exponential backoff

@dataclass
class RequestThrottler(_RequestThrottlerDefaultsBase):
    """
    A class that throttles requests with exponential backoff, jitter, and dynamic throttling thresholds.

    Attributes:
        max_requests_in_window (int): The maximum number of requests allowed within a single time window. Default is 10.
        rate_limit_window (int): The duration of the time window in seconds. Default is 1 second.
        throttle_start_percentage (float): The percentage of the max requests in the window at which throttling begins.
                                            Default is 0.75 (75%).
        full_throttle_percentage (float): The percentage of the max requests in the window at which full throttling occurs.
                                          Default is 0.90 (90%).
    """
    
    throttle_trigger_count: int = field(init=False)
    full_throttle_trigger_count: int = field(init=False)
    request_timestamps: deque = field(default_factory=deque, init=False)
    total_requests_made: int = field(default=0, init=False)
    window_start_time: float = field(default_factory=time.time, init=False)
    request_position: int = field(default=0, init=False)
    is_server_providing_request_position: bool = field(default=False, init=False)

    def __post_init__(self):
        """Initialize the throttler with the default values and calculate the throttle thresholds."""
        if self.max_requests_in_window <= 0:
            raise ValueError("max_requests_in_window must be greater than 0")
        if self.rate_limit_window <= 0:
            raise ValueError("rate_limit_window must be greater than 0")
        if not (0 <= self.throttle_start_percentage <= 1):
            raise ValueError("throttle_start_percentage must be between 0 and 1")
        if not (0 <= self.full_throttle_percentage <= 1):
            raise ValueError("full_throttle_percentage must be between 0 and 1")
        if not (0 <= self.full_throttle_buffer):
            raise ValueError("full_throttle_buffer must be greater than or equal to 0")
        if not (0 <= self.backoff_retries):
            raise ValueError("backoff_retries must be greater than or equal to 0")
        if not (self.backoff_base_delay > 0):
            raise ValueError("backoff_base_delay must be greater than 0")
        if not (self.backoff_factor > 1):
            raise ValueError("backoff_factor must be greater than 1")
        if not (self.backoff_max_delay > 0):
            raise ValueError("backoff_max_delay must be greater than 0")
        self._calculate_throttle_thresholds()
    
    def _calculate_throttle_thresholds(self):
        """Recalculate the throttle and full throttle trigger counts based on the current rate limits."""
        self.throttle_trigger_count = math.floor(self.max_requests_in_window * self.throttle_start_percentage)
        self.full_throttle_trigger_count = math.floor(self.max_requests_in_window * self.full_throttle_percentage)

    def _throttle(self):
        """Handle the throttling logic before making a request."""

        # Manage the request timestamps
        current_time = time.time()
        threshold_time = current_time - self.rate_limit_window
        while self.request_timestamps and self.request_timestamps[0] < threshold_time:
            self.request_timestamps.popleft()

        # Calculate the time remaining in the current window
        time_elapsed = current_time - self.window_start_time
        time_remaining = self.rate_limit_window - time_elapsed

        # Reset window start time if the current window has expired
        if time_remaining <= 0:
            self.window_start_time = current_time
            time_remaining = abs(self.rate_limit_window - time_elapsed)

        # Get the position of the current request in the throttling window
        if not self.is_server_providing_request_position:
            self.request_position = len(self.request_timestamps)

        # Apply backoff if the request count exceeds the full throttle trigger count
        is_within_rate_limit_window = time_elapsed < self.rate_limit_window
        has_count_exceeded_max_requests = self.request_position >= self.max_requests_in_window
        if has_count_exceeded_max_requests and is_within_rate_limit_window:
            self._skip_one_window()
            return
        
        # Apply full throttle if at or beyond the last position in the throttle range
        has_reached_full_throttle = self.request_position >= self.full_throttle_trigger_count
        if has_reached_full_throttle:
            self._apply_full_throttle(time_remaining)
            return

        # Apply throttling if within the throttle range
        is_within_throttle_range = self.throttle_trigger_count <= self.request_position < self.full_throttle_trigger_count
        if is_within_throttle_range:
            self._apply_throttle(time_remaining)
            return


    def _skip_one_window(self):
        print(f"\033[93m[Skip Window] Waiting {self.rate_limit_window:.2f} seconds before proceeding.\033[0m")
        time.sleep(self.rate_limit_window)


    def _apply_full_throttle(self, time_remaining):
        time_to_wait = time_remaining * (self.full_throttle_buffer + 1)
        if time_to_wait > 0:
            print(f"\033[93m[Full Throttle] Waiting {time_to_wait:.2f} seconds to consume remaining time.\033[0m")
            time.sleep(time_to_wait)


    def _apply_throttle(self, time_remaining):
        remaining_requests = self.full_throttle_trigger_count - self.request_position
        time_to_wait = min(time_remaining / max(remaining_requests, 1), self.rate_limit_window)
        print(f"\033[93m[Throttling] Waiting {time_to_wait:.2f} seconds before making the next request.\033[0m")
        time.sleep(time_to_wait)


    def _record_request(self):
        """Record the current time as a request timestamp and update the total request count."""
        self.request_timestamps.append(time.time())
        
        # Reset window start time if this is the first request in a new cycle
        if len(self.request_timestamps) == 1:
            self.window_start_time = time.time()


    def _is_transient_error(self, status_code, response):
        """
        Determine if the error is transient and worth retrying.
        
        Transient errors include:
        - 408: Request Timeout
        - 429: Too Many Requests
        - 5xx: Server Errors
        - 403: Forbidden (with 'Retry-After' header)
        """
        transient_errors = {408, 429}
        if status_code in transient_errors:
            return True
        if 500 <= status_code < 600:
            return True
        if status_code == 403 and 'Retry-After' in response.headers:
            return True
        return False
    
    
    def _make_request(self, method, url, headers=None, params=None, data=None, json=None):
        """Make a request with retries using exponential backoff and jitter."""
        headers = headers or {}
        params = params or {}
        data = data or {}
        json = json or {}
    
        method_map = {
            'GET': requests.get,
            'POST': requests.post,
            'PUT': requests.put,
            'PATCH': requests.patch,
            'DELETE': requests.delete
        }
    
        if method not in method_map:
            raise ValueError("Unsupported HTTP method")
    
        for attempt in range(self.backoff_retries):
            self._throttle()
    
            # Make the request
            try:
                response = method_map[method](url, headers=headers, params=params, data=data, json=json)

                try:
                    response.raise_for_status()
                except Exception as e:
                    pprint(response.headers)
                    raise e
                self._record_request()
                return response
    
            # Handle HTTP errors
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTPError: {http_err}")
                if not self._is_transient_error(http_err.response.status_code, http_err.response):
                    raise

                retry_after_header = http_err.response.headers.get('Retry-After')
                
                if retry_after_header:
                    try:
                        # Try to parse as an integer (seconds)
                        retry_after = int(retry_after_header)
                        print(f"[Retry-After] Retrying after {retry_after} seconds.")
                        time.sleep(retry_after)
                    except ValueError:
                        # If parsing as an integer fails, try to parse as a date
                        date_formats = [
                            '%a, %d %b %Y %H:%M:%S %Z',  # RFC 1123 format
                            '%A, %d-%b-%y %H:%M:%S %Z',  # RFC 850 format
                            '%a %b %d %H:%M:%S %Y'       # ANSI C's asctime() format
                        ]
                        retry_after_seconds = None
                        for date_format in date_formats:
                            try:
                                retry_after_date = datetime.strptime(retry_after_header, date_format)
                                retry_after_seconds = (retry_after_date - datetime.utcnow()).total_seconds()
                                break
                            except ValueError:
                                continue

                        if retry_after_seconds is not None and retry_after_seconds > 0:
                            print(f"[Retry-After] Retrying after {retry_after_seconds} seconds (parsed from date).")
                            time.sleep(retry_after_seconds)
                        else:
                            # Fall back to exponential backoff
                            print("[Retry-After] The retry date has already passed or could not be parsed.")
                            if attempt < self.backoff_retries:
                                sleep_time = (self.backoff_factor ** (attempt + 1)) + random.uniform(0, 1)
                                print(f"            Falling back to exponential backoff. Sleeping for {sleep_time} seconds.")
                                time.sleep(sleep_time)
                            else:
                                raise

                elif attempt < self.backoff_retries:
                    sleep_time = (self.backoff_factor ** (attempt + 1)) + random.uniform(0, 1)
                    print(f"[Back-off] Retrying after {sleep_time} seconds.")
                    time.sleep(sleep_time)
                else:
                    raise
    
            except requests.exceptions.RequestException as req_err:
                print(f"RequestException: {req_err}")
                if attempt < self.backoff_retries:
                    sleep_time = (self.backoff_factor ** attempt + 1) + random.uniform(0, 1)
                    print(f"[RequestException] Retrying after {sleep_time} seconds.")
                    time.sleep(sleep_time)
                else:
                    raise

    def throttled_get(self, url, headers=None, params=None):
        """Throttled GET request."""
        return self._make_request('GET', url, headers=headers, params=params)

    def throttled_post(self, url, data=None, json=None, headers=None, params=None):
        """Throttled POST request."""
        return self._make_request('POST', url, headers=headers, params=params, data=data, json=json)

    def throttled_put(self, url, data=None, headers=None, params=None):
        """Throttled PUT request."""
        return self._make_request('PUT', url, headers=headers, params=params, data=data)

    def throttled_patch(self, url, data=None, headers=None, params=None):
        """Throttled PATCH request."""
        return self._make_request('PATCH', url, headers=headers, params=params, data=data)

    def throttled_delete(self, url, headers=None, params=None):
        """Throttled DELETE request."""
        return self._make_request('DELETE', url, headers=headers, params=params)
