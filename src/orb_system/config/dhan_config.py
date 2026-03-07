import os
from dhanhq import dhanhq
from dotenv import load_dotenv

load_dotenv()

class DhanConfig:
    """DhanHQ API configuration for ORB System"""
    
    def __init__(self):
        self.client_id = os.getenv('DHAN_CLIENT_ID')
        self.access_token = os.getenv('DHAN_ACCESS_TOKEN')
        
        if not self.client_id or not self.access_token:
            # Fallback to defaults or log warning if testing
            print("Warning: DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN not found in environment")
        
        # Initialize Dhan client if tokens are present
        if self.client_id and self.access_token:
            self.dhan = dhanhq(self.client_id, self.access_token)
        else:
            self.dhan = None
    
    def get_client(self):
        """Get DhanHQ client instance"""
        if not self.dhan:
             # Try refreshing
             self.client_id = os.getenv('DHAN_CLIENT_ID')
             self.access_token = os.getenv('DHAN_ACCESS_TOKEN')
             if self.client_id and self.access_token:
                 self.dhan = dhanhq(self.client_id, self.access_token)
        return self.dhan

# Global Dhan instance
dhan_client = DhanConfig()
