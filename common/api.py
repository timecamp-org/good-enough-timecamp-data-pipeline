import time
import requests
import warnings
from typing import Dict, List, Any, Optional
from common.logger import setup_logger
from common.utils import TimeCampConfig

# Suppress SSL verification warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

logger = setup_logger('timecamp_sync')

class TimeCampAPI:
    def __init__(self, config: TimeCampConfig):
        self.base_url = f"https://{config.domain}/third_party/api"
        self.headers = {"Accept": "application/json", "Content-Type": "application/json", "Authorization": f"Bearer {config.api_key}"}

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        max_retries, retry_delay = 5, 5
        
        logger.debug(f"API Request: {method} {url}")

        for attempt in range(max_retries):
            try:
                response = requests.request(method, url, headers=self.headers, verify=False, **kwargs)
                logger.debug(f"Response status: {response.status_code}")
                # logger.debug(f"Response headers: {dict(response.headers)}")
                # logger.debug(f"Response content: {response.text[:1000]}")  # First 1000 chars to avoid huge logs
                
                if response.status_code == 429 and attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if getattr(e.response, 'status_code', None) == 429 and attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                logger.error(f"API Error: {method} {url} - Status: {getattr(e.response, 'status_code', 'N/A')}")
                if hasattr(e.response, 'text'):
                    logger.error(f"Error response: {e.response.text}")
                raise
        raise requests.exceptions.RequestException(f"Failed after {max_retries} retries")

    def get_users(self) -> List[Dict[str, Any]]:
        """Get all users with their enabled status."""
        users = self._make_request('GET', "users").json()
        # logger.debug(f"Users: {users}")

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
        
        logger.debug(f"Fetching time entries from {from_date} to {to_date} with params: {params}")
        response = self._make_request('GET', "entries", params=params)
        entries = response.json()
        
        logger.debug(f"Retrieved {len(entries)} time entries")
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