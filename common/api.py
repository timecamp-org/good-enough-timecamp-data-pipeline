import time
import requests
import warnings
import json
import os
from typing import Dict, List, Any, Optional
from common.logger import setup_logger
from common.utils import TimeCampConfig

# Suppress SSL verification warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

class TimeCampAPI:
    def __init__(self, config: TimeCampConfig, debug: bool = False):
        self.base_url = f"https://{config.domain}/third_party/api"
        self.headers = {"Accept": "application/json", "Content-Type": "application/json", "Authorization": f"Bearer {config.api_key}"}
        self.applications_cache_file = "applications_cache.json"
        self.logger = setup_logger('timecamp_sync', debug)

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        max_retries, retry_delay = 5, 5
        
        self.logger.debug(f"API Request: {method} {url}")

        for attempt in range(max_retries):
            try:
                response = requests.request(method, url, headers=self.headers, verify=False, **kwargs)
                self.logger.debug(f"Response status: {response.status_code}")
                # self.logger.debug(f"Response headers: {dict(response.headers)}")
                # self.logger.debug(f"Response content: {response.text[:1000]}")  # First 1000 chars to avoid huge logs
                
                if response.status_code == 429 and attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if getattr(e.response, 'status_code', None) == 429 and attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                self.logger.error(f"API Error: {method} {url} - Status: {getattr(e.response, 'status_code', 'N/A')}")
                if hasattr(e.response, 'text'):
                    self.logger.error(f"Error response: {e.response.text}")
                raise
        raise requests.exceptions.RequestException(f"Failed after {max_retries} retries")

    def get_users(self) -> List[Dict[str, Any]]:
        """Get all users with their enabled status."""
        users = self._make_request('GET', "users").json()
        # self.logger.debug(f"Users: {users}")

        # Get enabled status for all users in bulk
        user_ids = [int(user['user_id']) for user in users]
        enabled_statuses = self.are_users_enabled(user_ids)
        
        # Add enabled status to each user
        for user in users:
            user['is_enabled'] = enabled_statuses.get(int(user['user_id']), True)
        
        return users

    def get_time_entries(self, from_date: str, to_date: str, user_ids: Optional[List[int]] = None, 
                      include_project: bool = True, include_rates: bool = True,
                      opt_fields: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get time entries for specified date range.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            user_ids: Optional list of user IDs to filter by
            include_project: Whether to include project data in the response
            include_rates: Whether to include rate information in the response
            opt_fields: Optional comma-separated list of additional fields to include (e.g. "tags,breadcrumbs")

        Returns:
            List of time entry dictionaries
        """
        params = {
            "from": from_date,
            "to": to_date,
            "format": "json",
            "include_project": "1" if include_project else "0",
            "include_rates": "1" if include_rates else "0"
        }
        
        if user_ids:
            params["user_ids"] = ",".join(map(str, user_ids))
            
        if opt_fields:
            params["opt_fields"] = opt_fields
        
        self.logger.debug(f"Fetching time entries from {from_date} to {to_date} with params: {params}")
        response = self._make_request('GET', "entries", params=params)
        entries = response.json()
        
        self.logger.debug(f"Retrieved {len(entries)} time entries")
        return entries

    def get_groups(self) -> List[Dict[str, Any]]:
        return self._make_request('GET', "group").json()

    def get_group_users(self, group_id: int) -> List[Dict[str, Any]]:
        return self._make_request('GET', f"group/{group_id}/user").json()

    def are_users_enabled(self, user_ids: List[int], batch_size: int = 50) -> Dict[int, bool]:
        """Check if multiple users are enabled in bulk."""
        results = self.get_user_settings(user_ids, 'disabled_user', batch_size)
        # Convert 'disabled_user' values to boolean 'is_enabled' values
        return {user_id: not (str(value) == '1') for user_id, value in results.items()}

    def get_user_roles(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Get roles for all users across all groups.
        
        Returns:
            Dict mapping user_id to list of group assignments with their role_ids
            Example: {
                "1234": [{"group_id": "5678", "role_id": "2"}]
            }
        """
        response = self._make_request('GET', "people_picker")
        data = response.json()
        
        user_roles = {}
        
        # Process groups and their users
        for group_key, group_data in data.get('groups', {}).items():
            group_id = group_data.get('group_id')
            users = group_data.get('users', {})
            
            # Handle different format of users (dict vs list)
            if isinstance(users, dict):
                for user_id, user_data in users.items():
                    if user_id not in user_roles:
                        user_roles[user_id] = []
                    
                    user_roles[user_id].append({
                        'group_id': group_id,
                        'role_id': user_data.get('role_id')
                    })
            elif isinstance(users, list):
                # Empty users list or alternative format
                pass
        
        return user_roles

    def get_user_details(self) -> Dict[str, Any]:
        """
        Get detailed information about all users in the system, including their groups.
        
        Returns:
            Dict containing user details and group structure with all hierarchy levels
        """
        response = self._make_request('GET', "people_picker")
        return response.json()

    def get_user_settings(self, user_ids: List[int], setting_name: str, batch_size: int = 50) -> Dict[int, Optional[str]]:
        """Get specific user settings for multiple users in bulk."""
        result = {}
        for i in range(0, len(user_ids), batch_size):
            batch = user_ids[i:i + batch_size]
            response = self._make_request('GET', f"user/{','.join(map(str, batch))}/setting", 
                                        params={"name[]": setting_name})
            settings = response.json()
            
            # Handle both possible API response formats
            if isinstance(settings, dict):
                # New API format where settings is a dict with user_id keys
                for user_id in batch:
                    user_settings = settings.get(str(user_id), [])
                    if isinstance(user_settings, list):
                        setting_value = next(
                            (s.get('value') for s in user_settings 
                             if s.get('name') == setting_name),
                            None
                        )
                        result[user_id] = setting_value
                    else:
                        result[user_id] = None
            else:
                # Old API format where settings is a list
                for user_id in batch:
                    user_settings = [s for s in settings 
                                   if str(s.get('userId', '')) == str(user_id) 
                                   and s.get('name') == setting_name]
                    result[user_id] = user_settings[0].get('value') if user_settings else None
        
        return result

    def get_computer_activities(self, dates: List[str], include: Optional[str] = None, 
                              user_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """Get computer activities for specified dates.
        
        Note: TimeCamp API requires separate calls for each user, so this method
        automatically handles multiple user requests by making individual calls
        and combining the results. Also handles date ranges larger than 20 days
        by automatically batching into multiple requests.
        
        Args:
            dates: List of dates in YYYY-MM-DD format (automatically batched if > 20 dates)
            include: Optional comma-separated list of additional fields to include 
                    (e.g. "application,window_title")
            user_ids: Optional list of user IDs to filter by
            
        Returns:
            List of computer activity dictionaries
        """
        # Handle date batching if more than 20 dates
        if len(dates) > 20:
            self.logger.info(f"Date range contains {len(dates)} days, batching into chunks of 20 days")
            all_activities = []
            
            # Split dates into chunks of 20
            for i in range(0, len(dates), 20):
                date_batch = dates[i:i + 20]
                self.logger.debug(f"Processing date batch {i//20 + 1}: {len(date_batch)} dates from {date_batch[0]} to {date_batch[-1]}")
                
                try:
                    batch_activities = self._get_computer_activities_batched(date_batch, include, user_ids)
                    all_activities.extend(batch_activities)
                    self.logger.info(f"Retrieved {len(batch_activities)} activities for date batch {i//20 + 1}")
                except Exception as e:
                    self.logger.warning(f"Failed to get activities for date batch {i//20 + 1}: {e}")
                    # Continue with other batches even if one fails
                    continue
            
            self.logger.info(f"Combined total: {len(all_activities)} computer activities from {len(dates)} days")
            return all_activities
        else:
            # Single batch (20 or fewer dates)
            return self._get_computer_activities_batched(dates, include, user_ids)

    def _get_computer_activities_batched(self, dates: List[str], include: Optional[str] = None, 
                                       user_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """Handle computer activities for a single batch of dates (20 or fewer).
        
        This method handles the user batching logic for a single date range.
        """
        if len(dates) > 20:
            raise ValueError("Internal error: _get_computer_activities_batched called with > 20 dates")
        
        # If no user_ids specified or only one user, use single request
        if not user_ids or len(user_ids) == 1:
            return self._get_computer_activities_single_request(dates, include, user_ids)
        
        # Multiple users: make separate requests for each user and combine results
        self.logger.debug(f"Making separate API calls for {len(user_ids)} users due to API limitations")
        all_activities = []
        
        for user_id in user_ids:
            try:
                user_activities = self._get_computer_activities_single_request(
                    dates, include, [user_id]
                )
                all_activities.extend(user_activities)
                self.logger.debug(f"Retrieved {len(user_activities)} activities for user {user_id}")
            except Exception as e:
                self.logger.warning(f"Failed to get activities for user {user_id}: {e}")
                # Continue with other users even if one fails
                continue
        
        self.logger.debug(f"Batch total: {len(all_activities)} computer activities from {len(user_ids)} users")
        return all_activities
    
    def _get_computer_activities_single_request(self, dates: List[str], include: Optional[str] = None, 
                                              user_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """Make a single API request for computer activities.
        
        Internal method to handle individual API calls.
        """
        params = {}
        
        # Add dates as array parameters
        for i, date in enumerate(dates):
            params[f"dates[{i}]"] = date
            
        if include:
            params["include"] = include
            
        if user_ids:
            params["user_id"] = ",".join(map(str, user_ids))
        
        self.logger.debug(f"Fetching computer activities for dates {dates}, user_ids: {user_ids}")
        response = self._make_request('GET', "activity", params=params)
        activities = response.json()
        
        self.logger.debug(f"Retrieved {len(activities)} computer activities")
        return activities

    def get_applications(self, application_ids: List[str], date: Optional[str] = None, 
                        batch_size: int = 200) -> Dict[str, Dict[str, Any]]:
        """Get application details for specified application IDs.
        
        Args:
            application_ids: List of application IDs to fetch
            date: Optional date for filtering (YYYY-MM-DD format)
            batch_size: Number of application IDs to process per batch (default: 200)
            
        Returns:
            Dict mapping application_id to application details
        """
        all_apps = {}
        
        # Process application IDs in batches
        for i in range(0, len(application_ids), batch_size):
            batch = application_ids[i:i + batch_size]
            
            params = {
                "application_ids": ",".join(batch)
            }
            
            if date:
                params["date"] = date
            
            self.logger.debug(f"Fetching applications batch {i//batch_size + 1}: {len(batch)} application IDs")
            response = self._make_request('GET', "application", params=params)
            apps_batch = response.json()
            
            # Merge the batch results
            if isinstance(apps_batch, dict):
                all_apps.update(apps_batch)
            
        self.logger.debug(f"Retrieved {len(all_apps)} application details")
        return all_apps

    def _load_applications_cache(self) -> Dict[str, Dict[str, Any]]:
        """Load applications cache from file."""
        if not os.path.exists(self.applications_cache_file):
            self.logger.debug("Applications cache file does not exist, starting with empty cache")
            return {}
        
        try:
            with open(self.applications_cache_file, 'r') as f:
                cache = json.load(f)
                self.logger.debug(f"Loaded applications cache with {len(cache)} entries")
                return cache
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Failed to load applications cache: {e}. Starting with empty cache")
            return {}

    def _save_applications_cache(self, cache: Dict[str, Dict[str, Any]]) -> None:
        """Save applications cache to file."""
        try:
            with open(self.applications_cache_file, 'w') as f:
                json.dump(cache, f, indent=2)
                self.logger.debug(f"Saved applications cache with {len(cache)} entries")
        except IOError as e:
            self.logger.error(f"Failed to save applications cache: {e}")

    def get_applications_with_cache(self, application_ids: List[str], date: Optional[str] = None, 
                                  batch_size: int = 200) -> Dict[str, Dict[str, Any]]:
        """Get application details for specified application IDs with caching.
        
        Args:
            application_ids: List of application IDs to fetch
            date: Optional date for filtering (YYYY-MM-DD format)
            batch_size: Number of application IDs to process per batch (default: 200)
            
        Returns:
            Dict mapping application_id to application details
        """
        # Load existing cache
        cache = self._load_applications_cache()
        
        # Determine which application IDs are missing from cache
        missing_ids = [app_id for app_id in application_ids if app_id not in cache]
        
        self.logger.debug(f"Total application IDs requested: {len(application_ids)}")
        self.logger.debug(f"Found in cache: {len(application_ids) - len(missing_ids)}")
        self.logger.debug(f"Missing from cache: {len(missing_ids)}")
        
        # Fetch missing applications from API if any
        if missing_ids:
            self.logger.info(f"Fetching {len(missing_ids)} missing applications from API")
            new_apps = self.get_applications(missing_ids, date, batch_size)
            
            # Update cache with newly fetched applications
            cache.update(new_apps)
            
            # Save updated cache
            self._save_applications_cache(cache)
        else:
            self.logger.info("All requested applications found in cache, no API calls needed")
        
        # Return only the requested application IDs from cache
        result = {app_id: cache[app_id] for app_id in application_ids if app_id in cache}
        
        self.logger.debug(f"Returning {len(result)} application details")
        return result 