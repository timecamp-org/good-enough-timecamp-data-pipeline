import os
from dataclasses import dataclass
from typing import Optional, List
from dotenv import load_dotenv

@dataclass
class TimeCampConfig:
    api_key: str
    domain: str = 'app.timecamp.com'
    root_group_id: int = 0
    ignored_user_ids: List[int] = None
    use_supervisor_groups: bool = False
    skip_departments: Optional[str] = None

    def __post_init__(self):
        if self.ignored_user_ids is None:
            self.ignored_user_ids = []

    @classmethod
    def from_env(cls):
        load_dotenv()
        return cls(
            api_key=os.getenv("TIMECAMP_API_KEY", ""),
            domain=os.getenv("TIMECAMP_DOMAIN", "app.timecamp.com"),
            root_group_id=int(os.getenv("TIMECAMP_ROOT_GROUP_ID", "0"))
        )

def get_yesterday():
    """Get yesterday's date in YYYY-MM-DD format"""
    from datetime import datetime, timedelta
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def parse_date(date_str):
    """Parse date string to YYYY-MM-DD format"""
    from datetime import datetime
    
    if date_str.lower() == 'yesterday':
        return get_yesterday()
    
    # Try to parse the date string
    try:
        # Handle common formats
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%m-%d-%Y']:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        # If we get here, none of the formats worked
        raise ValueError(f"Could not parse date: {date_str}")
    except Exception as e:
        raise ValueError(f"Invalid date format: {date_str}. Error: {str(e)}")
