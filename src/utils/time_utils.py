"""Time utility functions for Toast ETL Pipeline."""

import re
from typing import Optional


def convert_to_minutes(time_str: Optional[str]) -> str:
    """
    Converts a time string like 'X hours, Y minutes, Z seconds' into total minutes.

    Parameters:
        time_str: Input time string or None.
        
    Returns:
        Total minutes up to 1 decimal place as a string.
    """
    if not time_str or str(time_str).strip() == "":
        return "0.0"
    
    try:
        # Normalize the string to lowercase
        time_str = str(time_str).lower()
        
        # Define regex patterns for hours, minutes, and seconds
        hour_pattern = r"(\d+)\s*hour"
        minute_pattern = r"(\d+)\s*minute"
        second_pattern = r"(\d+)\s*second"
        
        # Find all matches in the string
        hours = re.findall(hour_pattern, time_str)
        minutes = re.findall(minute_pattern, time_str)
        seconds = re.findall(second_pattern, time_str)
        
        # Convert matches to integers, defaulting to 0 if not found
        hours = int(hours[0]) if hours else 0
        minutes = int(minutes[0]) if minutes else 0
        seconds = int(seconds[0]) if seconds else 0
        
        # Calculate total minutes
        total_minutes = hours * 60 + minutes + seconds / 60
        return f"{total_minutes:.1f}"  # Format to 1 decimal place
        
    except Exception as e:
        print(f"Error converting time: '{time_str}', Error: {e}")
        return "0.0"  # Return 0.0 minutes if an error occurs 