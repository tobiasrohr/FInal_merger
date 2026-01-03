#!/usr/bin/env python3
"""
Merge Monday.com Boards

Merges items from source board into target board:
- Updates existing items (duplicates) with missing information
- Creates new items for non-duplicates
- Respects merge strategies (only_if_empty, overwrite, etc.)
"""

import os
import sys
import json
import yaml
import time
import re
import math
import tempfile
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dotenv import load_dotenv
from export_boards import MondayAPIClient
from build_duplicate_index import find_duplicate, extract_email_from_column_value, extract_hf4u_number

SOURCE_BOARD_ID = "9661290405"
TARGET_BOARD_ID = "3567618324"
MAVM_BOARD_ID = "7076404604"  # Board 2. MA/VM
NEW_GROUP_ID = "group_mkz7xyf0"  # Gruppe "Neu" im Quellboard für neue Items
SOURCE_DUPLICATE_RELATION_COLUMN_ID = "board_relation_mkz88sa1"  # Connect-Board-Spalte zum Verlinken von Duplikaten

# Mapping files per target board
BOARD_MAPPING_FILES = {
    TARGET_BOARD_ID: "column_mapping.yaml",
    MAVM_BOARD_ID: "column_mapping_mavm.yaml"
}

# Batch size for mutations (Monday.com limit is 50)
BATCH_SIZE = 50

# City coordinates for nearest city calculation (lat, lng)
CITY_COORDINATES = {
    "Aachen": (50.7753, 6.0839),
    "Aalen": (48.8378, 10.0933),
    "Abensberg": (48.8167, 11.85),
    "Abstatt": (49.0667, 9.3),
    "Altomünster": (48.3833, 11.25),
    "Altötting": (48.2269, 12.6758),
    "Amberg": (49.4403, 11.8633),
    "Aschaffenburg": (49.9769, 9.158),
    "Augsburg": (48.3705, 10.8978),
    "Bad Saulgau": (48.0167, 9.5),
    "Bad Waldsee": (47.9203, 9.7536),
    "Bad Wiessee": (47.7133, 11.7133),
    "Baden-Baden": (48.7606, 8.2408),
    "Bamberg": (49.8988, 10.9028),
    "Bayreuth": (49.9456, 11.5713),
    "Berlin": (52.52, 13.405),
    "Bielefeld": (52.0302, 8.5325),
    "Bocholt": (51.8392, 6.6178),
    "Bochum": (51.4818, 7.2162),
    "Bonn": (50.7374, 7.0982),
    "Braunschweig": (52.2689, 10.5268),
    "Bremen": (53.0793, 8.8017),
    "Burghausen": (48.1692, 12.8314),
    "Chemnitz": (50.8278, 12.9214),
    "Cottbus": (51.7563, 14.3329),
    "Dachau": (48.2603, 11.4342),
    "Darmstadt": (49.8728, 8.6512),
    "Deggendorf": (48.8417, 12.9583),
    "Dießen am Ammersee": (47.95, 11.1),
    "Dingolfing": (48.6333, 12.5),
    "Dorfen": (48.2703, 12.1608),
    "Dortmund": (51.5136, 7.4653),
    "Dresden": (51.0504, 13.7373),
    "Duisburg": (51.4344, 6.7623),
    "Düsseldorf": (51.2277, 6.7735),
    "Eggenfelden": (48.4028, 12.7628),
    "Erding": (48.3064, 11.9069),
    "Erfurt": (50.9848, 11.0299),
    "Erlangen": (49.5897, 11.0078),
    "Eschborn": (50.1431, 8.5706),
    "Essen": (51.4556, 7.0116),
    "Frankfurt": (50.1109, 8.6821),
    "Freiburg": (47.999, 7.8421),
    "Freising": (48.4028, 11.7489),
    "Friedrichshafen": (47.65, 9.4833),
    "Fürstenfeldbruck": (48.1789, 11.255),
    "Fürth": (49.4774, 10.9886),
    "Garbsen": (52.4181, 9.5989),
    "Garmisch-Partenkirchen": (47.5, 11.0833),
    "Gelsenkirchen": (51.5177, 7.0857),
    "Gilching": (48.1053, 11.2942),
    "Görlitz": (51.1528, 14.9872),
    "Gütersloh": (51.9032, 8.3858),
    "Halle (Saale)": (51.4969, 11.9688),
    "Hamburg": (53.5511, 9.9937),
    "Hamm": (51.6739, 7.815),
    "Hannover": (52.3759, 9.732),
    "Heidelberg": (49.3988, 8.6724),
    "Heidenheim": (48.6761, 10.1544),
    "Heilbronn": (49.1427, 9.2109),
    "Immenstaad am Bodensee": (47.6667, 9.3667),
    "Ingolstadt": (48.7665, 11.4258),
    "Itzehoe": (53.925, 9.5153),
    "Jena": (50.9271, 11.5892),
    "Karlsruhe": (49.0069, 8.4037),
    "Kassel": (51.3127, 9.4797),
    "Kaufbeuren": (47.8803, 10.6222),
    "Kempten": (47.7267, 10.3153),
    "Kiel": (54.3233, 10.1228),
    "Kirchheim Unter Teck": (48.6472, 9.4528),
    "Kirchseeon": (48.0728, 11.8836),
    "Koblenz": (50.3569, 7.589),
    "Konstanz": (47.6779, 9.1732),
    "Krefeld": (51.3388, 6.5853),
    "Köln": (50.9375, 6.9603),
    "Landsberg am Lech": (48.0525, 10.8792),
    "Landshut": (48.5372, 12.1519),
    "Leipzig": (51.3397, 12.3731),
    "Leverkusen": (51.0459, 6.9844),
    "Lindau": (47.546, 9.684),
    "London": (51.5074, -0.1278),
    "Los Angeles": (34.0522, -118.2437),
    "Ludwigshafen": (49.4774, 8.4452),
    "Lübeck": (53.8655, 10.6866),
    "Lüdenscheid": (51.2167, 7.6333),
    "Magdeburg": (52.1205, 11.6276),
    "Mainz": (49.9929, 8.2473),
    "Mannheim": (49.4875, 8.466),
    "Montabaur": (50.4375, 7.8256),
    "Mönchengladbach": (51.1805, 6.4428),
    "Mühldorf am Inn": (48.2453, 12.5225),
    "Mühlheim an der Ruhr": (51.4275, 6.8825),
    "München": (48.1351, 11.582),
    "Münster": (51.9607, 7.6261),
    "New York": (40.7128, -74.006),
    "Nürnberg": (49.4521, 11.0767),
    "Offenbach": (50.0956, 8.7761),
    "Oldenburg": (53.1435, 8.2146),
    "Osnabrück": (52.2799, 8.0472),
    "Paderborn": (51.7189, 8.7575),
    "Paris": (48.8566, 2.3522),
    "Passau": (48.5667, 13.4319),
    "Pforzheim": (48.8922, 8.6989),
    "Potsdam": (52.3906, 13.0645),
    "Radolfzell": (47.7369, 8.97),
    "Ravensburg": (47.7823, 9.612),
    "Regensburg": (49.0134, 12.1016),
    "Reutlingen": (48.4914, 9.2043),
    "Rohrdorf": (47.7833, 12.1667),
    "Rosenheim": (47.8561, 12.1289),
    "Rostock": (54.0924, 12.0991),
    "Saalfeld": (50.6483, 11.3644),
    "Saarbrücken": (49.2402, 6.9969),
    "Saint-Quen": (48.9119, 2.3336),
    "San Clemente": (33.427, -117.612),
    "San Francisco": (37.7749, -122.4194),
    "Scheidegg": (47.5833, 9.85),
    "Schweinfurt": (50.0492, 10.2268),
    "Schwerin": (53.6355, 11.4012),
    "Singen": (47.7597, 8.8403),
    "Solingen": (51.1652, 7.0671),
    "Straubing": (48.8817, 12.5731),
    "Stuttgart": (48.7758, 9.1829),
    "Taufkirchen (Vils)": (48.35, 12.1333),
    "Thüringen": (50.9, 11.05),
    "Traunstein": (47.8683, 12.6433),
    "Tübingen": (48.5216, 9.0576),
    "Ulm": (48.4011, 9.9876),
    "Verden": (52.9231, 9.2336),
    "Villingen-Schweningen": (48.0603, 8.4575),
    "Weilheim": (47.8397, 11.1422),
    "Weimar": (50.9795, 11.3235),
    "Wendelstein": (49.3517, 11.1536),
    "Wien": (48.2082, 16.3738),
    "Wiesbaden": (50.0782, 8.2398),
    "Wolfratshausen": (47.9133, 11.4217),
    "Wolfsburg": (52.4227, 10.7865),
    "Wuppertal": (51.2562, 7.1508),
    "Würzburg": (49.7913, 9.9534),
}


class ColumnConverter:
    """Handles column value transformations."""
    
    @staticmethod
    def parse_salary_text_to_number(text: str) -> Optional[float]:
        """Parse salary from text format (e.g., '€ 45.000' -> 45000, '100K' -> 100000)."""
        if not text:
            return None
        
        # Remove currency symbols only (keep comma for decimal parsing in K pattern)
        cleaned = re.sub(r'[€$£]', '', text)
        
        candidates = []
        
        # Pattern 1: Numbers with K/k suffix (e.g., "100K", "100k", "85K", "75,5K")
        # This handles cases like "ca. 100K in VZ" -> 100000
        # Match number (with optional decimal via . or ,) followed by K/k
        k_pattern = r'(\d+(?:[.,]\d+)?)\s*[Kk](?![a-zA-Z])'
        k_matches = re.findall(k_pattern, cleaned)
        for match in k_matches:
            # Replace comma with dot for decimal parsing
            number_str = match.replace(',', '.')
            # Multiply by 1000 for K suffix
            candidates.append(float(number_str) * 1000)
        
        # Now remove commas and extra spaces for other patterns
        cleaned_no_comma = re.sub(r'[,\s]', '', cleaned)
        
        # Pattern 2: Numbers with dots as thousand separators (e.g., "45.000")
        dot_pattern = r'(\d{1,3}(?:\.\d{3})+)'
        dot_matches = re.findall(dot_pattern, cleaned_no_comma)
        for match in dot_matches:
            # Remove dots and convert
            number_str = match.replace('.', '')
            candidates.append(float(number_str))
        
        # Pattern 3: Plain numbers (without dots or K suffix)
        # Only add if not already covered by patterns above
        numbers = re.findall(r'\d+', cleaned_no_comma)
        for num_str in numbers:
            # Skip if this number was part of a K-pattern match
            is_k_number = any(num_str in match.replace(',', '.').replace('.', '') for match in k_matches)
            # Skip if this number was part of a dot-pattern match
            is_dot_number = any(num_str in match.replace('.', '') for match in dot_matches)
            
            if not is_k_number and not is_dot_number:
                candidates.append(float(num_str))
        
        if candidates:
            # Return the largest number (likely the salary)
            return max(candidates)
        
        return None
    
    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate the great-circle distance between two points on Earth using the Haversine formula.
        
        Args:
            lat1, lon1: Coordinates of point 1 (in degrees)
            lat2, lon2: Coordinates of point 2 (in degrees)
            
        Returns:
            Distance in kilometers
        """
        R = 6371  # Earth's radius in kilometers
        
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        # Haversine formula
        a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c
    
    @staticmethod
    def find_nearest_city(lat: float, lng: float) -> Optional[str]:
        """
        Find the nearest city from CITY_COORDINATES based on Haversine distance.
        
        Args:
            lat: Latitude of the location
            lng: Longitude of the location
            
        Returns:
            Name of the nearest city, or None if no cities available
        """
        if not CITY_COORDINATES:
            return None
        
        nearest_city = None
        min_distance = float('inf')
        
        for city_name, (city_lat, city_lng) in CITY_COORDINATES.items():
            distance = ColumnConverter.haversine_distance(lat, lng, city_lat, city_lng)
            if distance < min_distance:
                min_distance = distance
                nearest_city = city_name
        
        return nearest_city
    
    @staticmethod
    def extract_location_from_item(item: Dict, location_col_id: str) -> Optional[Tuple[float, float]]:
        """
        Extract lat/lng coordinates from a location column value.
        
        Args:
            item: Item data containing column_values
            location_col_id: ID of the location column
            
        Returns:
            Tuple of (lat, lng) or None if not available
        """
        for col_val in item.get("column_values", []):
            if col_val.get("id") == location_col_id:
                value = col_val.get("value")
                if value:
                    try:
                        parsed = json.loads(value) if isinstance(value, str) else value
                        lat = parsed.get("lat")
                        lng = parsed.get("lng")
                        if lat is not None and lng is not None:
                            return (float(lat), float(lng))
                    except:
                        pass
        return None
    
    @staticmethod
    def convert_monthly_netto_to_yearly_brutto(monthly_netto: float) -> float:
        """
        Convert monthly netto salary to yearly brutto salary.
        Formula: Monatsnetto × 18 = Jahresbrutto
        (Based on example: 2000 € Monatsnetto = 36.000 € Jahresbrutto)
        """
        return monthly_netto * 18
    
    @staticmethod
    def calculate_salary_from_multiple_sources(item: Dict, 
                                               yearly_brutto_col_id: str,
                                               monthly_netto_col_id: str) -> Optional[float]:
        """
        Calculate yearly brutto salary from multiple source columns.
        Priority:
        1. Use Jahresbruttogehalt if available
        2. Convert Monatsnettogehalt to Jahresbrutto if available
        3. Return None if neither available
        """
        yearly_brutto_value = None
        monthly_netto_value = None
        
        # Extract values from item
        for col_val in item.get("column_values", []):
            col_id = col_val.get("id")
            text = (col_val.get("text") or "").strip()
            
            if col_id == yearly_brutto_col_id and text:
                yearly_brutto_value = ColumnConverter.parse_salary_text_to_number(text)
            
            elif col_id == monthly_netto_col_id and text:
                monthly_netto_value = ColumnConverter.parse_salary_text_to_number(text)
        
        # Priority 1: Use yearly brutto if available
        if yearly_brutto_value:
            return yearly_brutto_value
        
        # Priority 2: Convert monthly netto to yearly brutto
        if monthly_netto_value:
            return ColumnConverter.convert_monthly_netto_to_yearly_brutto(monthly_netto_value)
        
        return None
    
    @staticmethod
    def gender_to_salutation(item: Dict, gender_col_id: str) -> Optional[int]:
        """
        Convert gender (Geschlecht) to salutation (Anrede) option ID.
        Mapping:
        - "weiblich" → "Frau" (Option ID 1)
        - "männlich" → "Herr" (Option ID 2)
        Returns option ID for target dropdown column.
        """
        # Extract gender value from item
        for col_val in item.get("column_values", []):
            if col_val.get("id") == gender_col_id:
                text = (col_val.get("text") or "").strip().lower()
                value = col_val.get("value", "")
                
                # Check text first
                if "weiblich" in text:
                    return 1  # Frau
                elif "männlich" in text:
                    return 2  # Herr
                
                # Check value (might be JSON with option ID)
                if value:
                    try:
                        value_data = json.loads(value) if isinstance(value, str) else value
                        if isinstance(value_data, dict):
                            # Check if it has ids array
                            ids = value_data.get("ids", [])
                            if ids:
                                option_id = ids[0] if isinstance(ids, list) else ids
                                # Map: 1=weiblich→Frau, 2=männlich→Herr
                                if option_id == 1:  # weiblich
                                    return 1  # Frau
                                elif option_id == 2:  # männlich
                                    return 2  # Herr
                    except:
                        pass
        
        return None
    
    @staticmethod
    def map_dropdown_values(item: Dict, source_col_id: str, value_mapping: Dict) -> Optional[List[str]]:
        """
        Map dropdown values from source to target using a value mapping dictionary.
        Supports multi-select dropdowns (comma-separated values).
        
        Args:
            item: Source item data
            source_col_id: Source column ID
            value_mapping: Dict mapping source values to target values
            
        Returns:
            List of mapped target values, or None if no mapping found
        """
        for col_val in item.get("column_values", []):
            if col_val.get("id") == source_col_id:
                text = (col_val.get("text") or "").strip()
                if not text:
                    return None
                
                # Split by comma for multi-select dropdowns
                source_values = [v.strip() for v in text.split(",") if v.strip()]
                
                # Map each value
                mapped_values = []
                for source_val in source_values:
                    target_val = value_mapping.get(source_val)
                    if target_val:
                        mapped_values.append(target_val)
                
                return mapped_values if mapped_values else None
        
        return None
    
    @staticmethod
    def parse_text_to_number(item: Dict, source_col_id: str) -> Optional[float]:
        """
        Parse a number from a text field.
        Handles various formats like "2", "2.5", "keine", empty strings.
        
        Returns:
            Float value or None if not parseable
        """
        for col_val in item.get("column_values", []):
            if col_val.get("id") == source_col_id:
                text = (col_val.get("text") or "").strip().lower()
                if not text or text in ["keine", "nein", "-", "n/a", "bitte wählen"]:
                    return None
                
                # Try to extract number
                try:
                    # Replace comma with dot for decimal
                    text = text.replace(",", ".")
                    # Extract first number found
                    number_match = re.search(r'[\d.]+', text)
                    if number_match:
                        return float(number_match.group())
                except (ValueError, AttributeError):
                    pass
                
                return None
        
        return None
    
    @staticmethod
    def map_country_text_to_label(item: Dict, source_col_id: str, value_mapping: Dict, 
                                   valid_countries: List[str]) -> Optional[str]:
        """
        Map a country text value to a dropdown label.
        Uses case-insensitive matching and normalization.
        
        Args:
            item: Source item data
            source_col_id: Source column ID
            value_mapping: Dict for special normalizations
            valid_countries: List of valid country names in target dropdown
            
        Returns:
            Country name matching target dropdown label, or None
        """
        for col_val in item.get("column_values", []):
            if col_val.get("id") == source_col_id:
                text = (col_val.get("text") or "").strip()
                if not text or text.lower() in ["bitte wählen", "-", "n/a", ""]:
                    return None
                
                # Check explicit mapping first (case-insensitive)
                text_lower = text.lower()
                for source_val, target_val in value_mapping.items():
                    if source_val.lower() == text_lower:
                        return target_val
                
                # Try to find direct match in valid countries (case-insensitive)
                for country in valid_countries:
                    if country.lower() == text_lower:
                        return country
                
                # Try partial match (for typos etc.)
                for country in valid_countries:
                    if text_lower in country.lower() or country.lower() in text_lower:
                        return country
                
                return None
        
        return None
    
    @staticmethod
    def convert_value(value: Any, transform_name: str, item: Optional[Dict] = None, 
                     mapping: Optional[Dict] = None, transformations: Optional[Dict] = None) -> Any:
        """Apply transformation to value."""
        if transform_name == "parse_salary":
            text = value.get("text", "") if isinstance(value, dict) else str(value)
            return ColumnConverter.parse_salary_text_to_number(text)
        
        elif transform_name == "calculate_salary":
            # This transformation needs the full item and mapping
            if item and mapping:
                yearly_col = mapping.get("source_yearly_column_id", "text_mktvfr1y")
                monthly_col = mapping.get("source_monthly_column_id", "text_mktvsm8z")
                return ColumnConverter.calculate_salary_from_multiple_sources(
                    item, yearly_col, monthly_col
                )
            return None
        
        elif transform_name == "gender_to_salutation":
            # This transformation needs the full item and mapping
            if item and mapping:
                gender_col = mapping.get("source_gender_column_id", "dropdown_mktvnt0e")
                return ColumnConverter.gender_to_salutation(item, gender_col)
            return None
        
        elif transform_name in ("map_hours", "map_languages", "map_familienstand", "map_nationalitaet"):
            # These transformations use value_mapping from the transformations config
            if item and mapping and transformations:
                source_col_id = mapping.get("source_column_id")
                transform_config = transformations.get(transform_name, {})
                value_mapping = transform_config.get("value_mapping", {})
                
                if source_col_id and value_mapping:
                    return ColumnConverter.map_dropdown_values(item, source_col_id, value_mapping)
            return None
        
        elif transform_name == "parse_number":
            # Parse number from text field
            if item and mapping:
                source_col_id = mapping.get("source_column_id")
                if source_col_id:
                    return ColumnConverter.parse_text_to_number(item, source_col_id)
            return None
        
        elif transform_name == "map_country":
            # Map country text to dropdown label
            if item and mapping and transformations:
                source_col_id = mapping.get("source_column_id")
                transform_config = transformations.get(transform_name, {})
                value_mapping = transform_config.get("value_mapping", {})
                
                # Valid countries list (same as in create_columns.py)
                valid_countries = [
                    "Afghanistan", "Albanien", "Algerien", "Angola", "Argentinien",
                    "Armenien", "Aserbaidschan", "Belgien", "Benin", "Bosnien und Herzegowina",
                    "Brasilien", "Bulgarien", "China", "Deutschland", "Dominikanische Republik",
                    "Ecuador", "Eritrea", "Estland", "Frankreich", "Georgien", "Ghana",
                    "Griechenland", "Großbritannien", "Indien", "Irak", "Iran", "Irland",
                    "Israel", "Italien", "Japan", "Jemen", "Jordanien", "Kamerun", "Kapverden",
                    "Kasachstan", "Kolumbien", "Kongo", "Kosovo", "Kroatien", "Kuba",
                    "Lettland", "Litauen", "Mazedonien", "Marokko", "Mexiko", "Mongolei",
                    "Neuseeland", "Nicaragua", "Niederlande", "Nigeria", "Norwegen",
                    "Pakistan", "Polen", "Portugal", "Rumänien", "Russland", "Schweden",
                    "Schweiz", "Senegal", "Serbien", "Slowakei", "Slowenien", "Somalia",
                    "Spanien", "Sudan", "Syrien", "Südafrika", "Taiwan", "Thailand", "Togo",
                    "Tschechien", "Tunesien", "Türkei", "USA", "Ukraine", "Ungarn",
                    "Usbekistan", "Weißrussland", "Zypern", "Ägypten", "Äthiopien", "Österreich"
                ]
                
                if source_col_id:
                    return ColumnConverter.map_country_text_to_label(
                        item, source_col_id, value_mapping, valid_countries
                    )
            return None
        
        elif transform_name == "map_nearest_city":
            # Map location coordinates to nearest city dropdown label
            if item and mapping:
                source_col_id = mapping.get("source_column_id")
                if source_col_id:
                    coords = ColumnConverter.extract_location_from_item(item, source_col_id)
                    if coords:
                        lat, lng = coords
                        nearest_city = ColumnConverter.find_nearest_city(lat, lng)
                        if nearest_city:
                            return [nearest_city]  # Return as list for dropdown
            return None
        
        return value


class BoardMerger:
    """Handles merging of boards."""
    
    def __init__(self, client: MondayAPIClient, mapping_configs: Dict[str, Dict], duplicate_index: Dict, duplicate_group_id: Optional[str] = None, new_group_id: Optional[str] = None):
        """
        Args:
            mapping_configs: Dict mapping board_id -> mapping_config
                             e.g. {"3567618324": config1, "7076404604": config2}
        """
        self.client = client
        self.mapping_configs = mapping_configs
        self.duplicate_index = duplicate_index
        self.duplicate_group_id = duplicate_group_id
        self.new_group_id = new_group_id
        # Collect all transformations from all configs
        self.transformations = {}
        for config in mapping_configs.values():
            self.transformations.update(config.get("transformations", {}))
        self.stats = {
            "created": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0,
            "moved_duplicates": 0,
            "moved_new": 0
        }
        self.log_entries = []
    
    def get_mapping_for_board(self, board_id: str) -> Dict:
        """Get mapping config for a specific board."""
        return self.mapping_configs.get(board_id, self.mapping_configs.get(TARGET_BOARD_ID, {}))
    
    def get_item_board_id(self, item_id: str) -> Optional[str]:
        """Get the board ID for an item."""
        query = """
        query GetItemBoard($itemId: ID!) {
            items(ids: [$itemId]) {
                board {
                    id
                }
            }
        }
        """
        try:
            result = self.client.execute_query(query, {"itemId": item_id})
            items = result.get("items", [])
            if items:
                return items[0].get("board", {}).get("id")
        except Exception as e:
            self.log_entries.append({
                "action": "get_board_error",
                "item_id": item_id,
                "error": str(e)
            })
        return None
    
    def copy_file_to_item(self, asset_id: str, target_item_id: str, 
                          target_column_id: str, filename: str) -> bool:
        """
        Copy a file from source asset to target item's file column.
        
        1. Get public_url for the asset via API
        2. Download file from public_url
        3. Upload to target item via add_file_to_column mutation
        
        Args:
            asset_id: Asset ID of the source file
            target_item_id: ID of the target item
            target_column_id: ID of the target file column
            filename: Name for the uploaded file
            
        Returns:
            True if successful, False otherwise
        """
        if not asset_id:
            return False
        
        try:
            # 1. Get public URL for the asset
            public_url = self.get_asset_public_url(asset_id)
            if not public_url:
                self.log_entries.append({
                    "action": "file_no_public_url",
                    "asset_id": asset_id,
                    "target_item_id": target_item_id
                })
                return False
            
            # 2. Download file from public URL (no auth needed)
            download_response = requests.get(public_url, timeout=60)
            
            if download_response.status_code != 200:
                self.log_entries.append({
                    "action": "file_download_error",
                    "asset_id": asset_id,
                    "status_code": download_response.status_code
                })
                return False
            
            # 3. Save to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{filename}") as tmp:
                tmp.write(download_response.content)
                tmp_path = tmp.name
            
            try:
                # 4. Upload to Monday.com via /v2/file endpoint
                mutation = f'''
                mutation ($file: File!) {{
                    add_file_to_column(
                        item_id: {target_item_id},
                        column_id: "{target_column_id}",
                        file: $file
                    ) {{
                        id
                    }}
                }}
                '''
                
                with open(tmp_path, 'rb') as f:
                    files = {
                        'query': (None, mutation),
                        'variables[file]': (filename, f, 'application/octet-stream')
                    }
                    
                    upload_response = requests.post(
                        "https://api.monday.com/v2/file",
                        headers={"Authorization": self.client.api_token},
                        files=files,
                        timeout=120
                    )
                
                if upload_response.status_code == 200:
                    result = upload_response.json()
                    if result.get("data", {}).get("add_file_to_column", {}).get("id"):
                        return True
                    elif result.get("errors"):
                        self.log_entries.append({
                            "action": "file_upload_error",
                            "item_id": target_item_id,
                            "column_id": target_column_id,
                            "error": str(result.get("errors"))[:200]
                        })
                else:
                    self.log_entries.append({
                        "action": "file_upload_error",
                        "item_id": target_item_id,
                        "column_id": target_column_id,
                        "status_code": upload_response.status_code
                    })
                
                return False
                
            finally:
                # Clean up temp file
                try:
                    os.unlink(tmp_path)
                except:
                    pass
                    
        except Exception as e:
            self.log_entries.append({
                "action": "file_copy_error",
                "item_id": target_item_id,
                "column_id": target_column_id,
                "error": str(e)[:200]
            })
            return False
    
    def extract_file_info(self, col_value: Dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Extract file info from a file column value.
        
        Returns:
            Tuple of (asset_id, filename, protected_url) or (None, None, None) if no file
        """
        if not col_value:
            return None, None, None
        
        # URL is in the 'text' field (protected URL, for reference)
        protected_url = (col_value.get("text") or "").strip()
        
        # Asset ID and filename from value JSON
        value = col_value.get("value")
        asset_id = None
        filename = "file"
        
        if value:
            try:
                parsed = json.loads(value) if isinstance(value, str) else value
                files = parsed.get("files", [])
                if files and len(files) > 0:
                    asset_id = files[0].get("assetId")
                    filename = files[0].get("name", "file")
            except:
                pass
        
        if not asset_id:
            return None, None, None
        
        return str(asset_id), filename, protected_url
    
    def get_asset_public_url(self, asset_id: str) -> Optional[str]:
        """Get the public download URL for an asset."""
        query = """
        query GetAsset($assetIds: [ID!]!) {
            assets(ids: $assetIds) {
                public_url
            }
        }
        """
        try:
            result = self.client.execute_query(query, {"assetIds": [asset_id]})
            assets = result.get("assets", [])
            if assets and len(assets) > 0:
                return assets[0].get("public_url")
        except Exception as e:
            self.log_entries.append({
                "action": "get_asset_error",
                "asset_id": asset_id,
                "error": str(e)[:100]
            })
        return None
    
    def move_item_to_group(self, item_id: str, board_id: str, group_id: str):
        """Move item to a specific group."""
        mutation = """
        mutation MoveItemToGroup($itemId: ID!, $groupId: String!) {
            move_item_to_group(
                item_id: $itemId,
                group_id: $groupId
            ) {
                id
            }
        }
        """
        
        variables = {
            "itemId": item_id,
            "groupId": group_id
        }
        
        try:
            self.client.execute_query(mutation, variables)
            return True
        except Exception as e:
            self.log_entries.append({
                "action": "move_error",
                "item_id": item_id,
                "error": str(e)
            })
            return False
    
    def link_source_to_duplicate(self, source_item_id: str, duplicate_item_id: str) -> bool:
        """
        Link source item to the found duplicate via board-relation column.
        
        Sets the Connect-Board column on the SOURCE item to point to the duplicate
        item in the target board (Bewerberliste or 2. MA/VM).
        
        Args:
            source_item_id: ID of the item in the source board
            duplicate_item_id: ID of the duplicate item in the target board
            
        Returns:
            True if successful, False otherwise
        """
        # Build the value for board-relation column (overwrite mode)
        relation_value = json.dumps({"item_ids": [int(duplicate_item_id)]})
        
        success = self.update_single_column(
            source_item_id,
            SOURCE_BOARD_ID,
            SOURCE_DUPLICATE_RELATION_COLUMN_ID,
            relation_value
        )
        
        if success:
            self.log_entries.append({
                "action": "link_duplicate",
                "source_item_id": source_item_id,
                "duplicate_item_id": duplicate_item_id,
                "column_id": SOURCE_DUPLICATE_RELATION_COLUMN_ID
            })
        else:
            self.log_entries.append({
                "action": "link_duplicate_error",
                "source_item_id": source_item_id,
                "duplicate_item_id": duplicate_item_id,
                "column_id": SOURCE_DUPLICATE_RELATION_COLUMN_ID
            })
        
        return success
            
    def create_update(self, item_id: str, body: str):
        """Create an update (comment) on an item."""
        mutation = """
        mutation CreateUpdate($itemId: ID!, $body: String!) {
            create_update(item_id: $itemId, body: $body) {
                id
            }
        }
        """
        variables = {
            "itemId": item_id,
            "body": body
        }
        try:
            self.client.execute_query(mutation, variables)
            time.sleep(0.1) # Small delay
        except Exception as e:
            self.log_entries.append({
                "action": "create_update_error",
                "item_id": item_id,
                "error": str(e)
            })

    def transfer_updates(self, source_item: Dict, target_item_id: str):
        """Transfer updates from source item to target item (combined into one)."""
        updates = source_item.get("updates", [])
        if not updates:
            return

        combined_parts = []
        
        # Updates come newest first, reverse to post oldest first so order is correct
        for update in reversed(updates):
            original_body = update.get("body", "")
            combined_parts.append(original_body)
            
        if combined_parts:
            # Combine all updates with a separator, add header
            updates_body = "<br><br><hr><br><br>".join(combined_parts)
            full_body = f"<strong>Übertrag HR4You</strong><br><br>{updates_body}"
            self.create_update(target_item_id, full_body)
    
    def get_column_value(self, item: Dict, column_id: str) -> Optional[Dict]:
        """Get column value from item by column ID."""
        for col_val in item.get("column_values", []):
            if col_val.get("id") == column_id:
                return col_val
        return None
    
    def is_empty(self, col_value: Optional[Dict]) -> bool:
        """Check if column value is empty.
        
        Uses 'text' as primary indicator - if no text is displayed to the user,
        the column is considered empty (even if internal 'value' has stale data
        like deleted dropdown labels).
        """
        if not col_value:
            return True
        
        text = (col_value.get("text") or "").strip()
        
        # If there's visible text, column is not empty
        if text:
            return False
        
        # No text = empty (even if value has stale/orphaned data)
        return True
    
    def get_column_type_from_value(self, col_value: Dict) -> str:
        """
        Infer column type from the value structure.
        Returns the column type or 'unknown'.
        """
        if not col_value:
            return "unknown"
        
        value = col_value.get("value")
        if not value:
            return "text"
        
        try:
            parsed = json.loads(value) if isinstance(value, str) else value
            if not isinstance(parsed, dict):
                return "text"
            
            # Detect by structure
            if "url" in parsed and "text" in parsed:
                return "link"
            if "email" in parsed:
                return "email"
            if "phone" in parsed:
                return "phone"
            if "date" in parsed:
                return "date"
            if "lat" in parsed and "lng" in parsed:
                return "location"
            if "linkedPulseIds" in parsed:
                return "board-relation"
            if "ids" in parsed:
                return "dropdown"
            if "index" in parsed:
                return "status"
            if "files" in parsed:
                return "file"
            
            return "text"
        except:
            return "text"
    
    def prepare_value_for_create(self, col_value: Dict, col_type: str, 
                                  transform: Optional[str] = None,
                                  item: Optional[Dict] = None,
                                  mapping: Optional[Dict] = None) -> Any:
        """
        Prepare a column value for create_item API.
        
        Returns Python objects that will be JSON-serialized later by json.dumps(column_values).
        
        Return types by column:
        - text: str
        - date: str "YYYY-MM-DD"
        - dropdown: dict {"labels": [...]} or {"ids": [...]}
        - link: dict {"url": "...", "text": "..."}
        - location: dict {"lat": X, "lng": Y, "address": "..."}
        - board-relation: dict {"item_ids": [...]}
        - status: dict {"index": N}
        - phone: dict {"phone": "...", "countryShortName": "..."}
        - numeric: str (number as string)
        """
        # Handle transformations first
        if transform:
            converted = ColumnConverter.convert_value(
                col_value, transform, item=item, mapping=mapping,
                transformations=self.transformations
            )
            if converted is not None:
                if col_type in ("numeric", "numbers"):
                    return str(converted)
                elif col_type == "dropdown":
                    if isinstance(converted, list):
                        return {"labels": converted}
                    elif isinstance(converted, int):
                        return {"ids": [str(converted)]}
                    else:
                        return {"labels": [str(converted)]}
                elif col_type == "text":
                    return str(converted)
            return None
        
        # No transformation - use original value
        text = (col_value.get("text") or "").strip()
        value = col_value.get("value")
        
        if not text and not value:
            return None
        
        # Format based on column type
        if col_type in ("text", "long-text", "name"):
            return text if text else None
        
        if col_type == "date":
            # Extract just the date string
            if value:
                try:
                    parsed = json.loads(value) if isinstance(value, str) else value
                    date_val = parsed.get("date")
                    if date_val:
                        return date_val  # Just the date string "YYYY-MM-DD"
                except:
                    pass
            return text if text else None
        
        if col_type == "link":
            if value:
                try:
                    parsed = json.loads(value) if isinstance(value, str) else value
                    return {
                        "url": parsed.get("url", ""),
                        "text": parsed.get("text", "")
                    }
                except:
                    pass
            return None
        
        if col_type == "status":
            if value:
                try:
                    parsed = json.loads(value) if isinstance(value, str) else value
                    if "index" in parsed:
                        return {"index": parsed["index"]}
                except:
                    pass
            return None
        
        if col_type == "dropdown":
            if value:
                try:
                    parsed = json.loads(value) if isinstance(value, str) else value
                    if "ids" in parsed:
                        return {"ids": parsed["ids"]}
                except:
                    pass
            # Fallback to label
            if text:
                return {"labels": [text]}
            return None
        
        if col_type == "location":
            if value:
                try:
                    parsed = json.loads(value) if isinstance(value, str) else value
                    return {
                        "lat": parsed.get("lat"),
                        "lng": parsed.get("lng"),
                        "address": parsed.get("address", "")
                    }
                except:
                    pass
            return None
        
        if col_type == "board-relation":
            if value:
                try:
                    parsed = json.loads(value) if isinstance(value, str) else value
                    linked_ids = parsed.get("linkedPulseIds", [])
                    if linked_ids:
                        item_ids = [p.get("linkedPulseId") for p in linked_ids if p.get("linkedPulseId")]
                        if item_ids:
                            return {"item_ids": item_ids}
                except:
                    pass
            return None
        
        if col_type == "phone":
            if value:
                try:
                    parsed = json.loads(value) if isinstance(value, str) else value
                    return {
                        "phone": parsed.get("phone", ""),
                        "countryShortName": parsed.get("countryShortName", "DE")
                    }
                except:
                    pass
            return None
        
        if col_type in ("numeric", "numbers"):
            return text if text else None
        
        # Default: return as-is if it's valid JSON, otherwise as text
        if value:
            try:
                parsed = json.loads(value) if isinstance(value, str) else value
                # Remove metadata like changed_at
                if isinstance(parsed, dict):
                    cleaned = {k: v for k, v in parsed.items() if k not in ["changed_at"]}
                    return cleaned
            except:
                pass
        
        return text if text else None
    
    def prepare_column_value(self, source_col_val: Dict, target_col_type: str, 
                            transform: Optional[str] = None, 
                            item: Optional[Dict] = None,
                            mapping: Optional[Dict] = None) -> Any:
        """Prepare column value for target board format."""
        if transform:
            converted = ColumnConverter.convert_value(
                source_col_val, transform, item=item, mapping=mapping, 
                transformations=self.transformations
            )
            if converted is not None:
                # Format based on target column type
                if target_col_type == "numeric" or target_col_type == "numbers":
                    # Monday.com expects just the number as a string for numeric columns
                    return str(converted)
                elif target_col_type == "dropdown":
                    # Dropdown columns need labels in format: {"labels": ["Label1", "Label2"]}
                    # For multi-select dropdowns, converted is a list of labels
                    if isinstance(converted, list):
                        return json.dumps({"labels": converted})
                    elif isinstance(converted, int):
                        return json.dumps({"ids": [str(converted)]})
                    else:
                        return json.dumps({"labels": [str(converted)]})
                elif target_col_type == "text":
                    return json.dumps({"text": str(converted)})
        
        # Use original value
        value = source_col_val.get("value")
        text = source_col_val.get("text", "")
        
        if value:
            return value
        elif text:
            return json.dumps({"text": text})
        
        return None
    
    def should_update_column(self, merge_strategy: str, target_col_value: Optional[Dict]) -> bool:
        """Determine if column should be updated based on merge strategy."""
        if merge_strategy == "overwrite":
            return True
        elif merge_strategy == "only_if_empty":
            return self.is_empty(target_col_value)
        elif merge_strategy == "append":
            # TODO: Implement append logic
            return False
        elif merge_strategy == "skip":
            return False
        
        return False
    
    def create_item(self, item: Dict, mappings: List[Dict], group_id: Optional[str] = None) -> Optional[str]:
        """
        Create new item in target board with proper column mapping and file handling.
        
        Process:
        1. Prepare column values using target column IDs from mapping
        2. Create the item (without file and email columns)
        3. Upload files to the created item
        4. Set email column separately (API workaround)
        """
        item_name = item.get("name", "")
        source_item_id = item.get("id")
        
        # Collect values by category
        column_values = {}  # For create_item API
        file_columns = []   # For separate file upload
        email_columns = []  # For separate email update
        
        # Skip these column types in initial creation
        SKIP_TYPES = ["file", "mirror", "formula", "creation_log", "auto_number", 
                      "button", "subtasks", "dependency", "doc"]
        
        for mapping in mappings:
            source_col_id = mapping.get("source_column_id")
            target_col_id = mapping.get("target_column_id")
            transform = mapping.get("transform")
            
            if not target_col_id:
                continue
            
            # Get source column value
            source_col_val = self.get_column_value(item, source_col_id)
            
            # Detect column type
            col_type = self.get_column_type_from_value(source_col_val) if source_col_val else "text"
            
            # Handle file columns separately
            if col_type == "file":
                if source_col_val:
                    asset_id, filename, _ = self.extract_file_info(source_col_val)
                    if asset_id:
                        file_columns.append({
                            "asset_id": asset_id,
                            "target_col_id": target_col_id,
                            "filename": filename
                        })
                continue
            
            # Handle email columns separately (API bug workaround)
            if col_type == "email":
                if source_col_val:
                    text = (source_col_val.get("text") or "").strip()
                    if text:
                        email_columns.append({
                            "target_col_id": target_col_id,
                            "email": text
                        })
                continue
            
            # Skip non-copyable column types
            if col_type in SKIP_TYPES:
                continue
            
            # Handle transformations
            if transform:
                # Transformations that need special handling
                if transform == "calculate_salary":
                    col_type = "numbers"
                elif transform == "gender_to_salutation":
                    col_type = "dropdown"
                elif transform in ("map_hours", "map_languages", "map_country", "map_familienstand", "map_nearest_city", "map_nationalitaet"):
                    col_type = "dropdown"
                elif transform == "parse_number":
                    col_type = "numbers"
                
                dummy_col_val = source_col_val or {"id": source_col_id, "text": "", "value": ""}
                prepared = self.prepare_value_for_create(
                    dummy_col_val, col_type, transform, item=item, mapping=mapping
                )
                if prepared:
                    column_values[target_col_id] = prepared
                continue
            
            # Standard columns
            if not source_col_val:
                continue
            
            prepared = self.prepare_value_for_create(
                source_col_val, col_type, transform=None, item=item, mapping=mapping
            )
            if prepared:
                column_values[target_col_id] = prepared
        
        # Create item mutation
        mutation = """
        mutation CreateItem($boardId: ID!, $itemName: String!, $columnValues: JSON!, $groupId: String) {
            create_item(
                board_id: $boardId,
                item_name: $itemName,
                column_values: $columnValues,
                group_id: $groupId,
                create_labels_if_missing: true
            ) {
                id
            }
        }
        """
        
        variables = {
            "boardId": TARGET_BOARD_ID,
            "itemName": item_name,
            "columnValues": json.dumps(column_values)
        }
        
        if group_id:
            variables["groupId"] = group_id
        
        try:
            result = self.client.execute_query(mutation, variables)
            new_item_id = result.get("create_item", {}).get("id")
            
            if not new_item_id:
                self.log_entries.append({
                    "action": "create_error",
                    "item_name": item_name,
                    "error": "No item ID returned"
                })
                return None
            
            # Upload files to the created item
            files_uploaded = 0
            for file_info in file_columns:
                if self.copy_file_to_item(
                    file_info["asset_id"],
                    new_item_id,
                    file_info["target_col_id"],
                    file_info["filename"]
                ):
                    files_uploaded += 1
                time.sleep(0.3)  # Rate limit for file uploads
            
            if file_columns:
                self.log_entries.append({
                    "action": "files_uploaded",
                    "item_id": new_item_id,
                    "uploaded": files_uploaded,
                    "total": len(file_columns)
                })
            
            # Set email columns separately
            for email_info in email_columns:
                success = self.update_single_column(
                    new_item_id, 
                    TARGET_BOARD_ID,
                    email_info["target_col_id"],
                    json.dumps({"email": email_info["email"], "text": email_info["email"]})
                )
                if not success:
                    self.log_entries.append({
                        "action": "email_transfer_failed",
                        "source_item_id": item.get("id"),
                        "target_item_id": new_item_id,
                        "item_name": item_name,
                        "email": email_info["email"],
                        "column_id": email_info["target_col_id"]
                    })
            
            return new_item_id
            
        except Exception as e:
            self.log_entries.append({
                "action": "create_error",
                "item_name": item_name,
                "error": str(e)[:200]
            })
            return None
    
    def update_single_column(self, item_id: str, board_id: str, column_id: str, value: str) -> bool:
        """Update a single column value."""
        mutation = """
        mutation ChangeColumnValue($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
            change_column_value(
                board_id: $boardId,
                item_id: $itemId,
                column_id: $columnId,
                value: $value,
                create_labels_if_missing: true
            ) {
                id
            }
        }
        """
        
        try:
            self.client.execute_query(mutation, {
                "boardId": board_id,
                "itemId": item_id,
                "columnId": column_id,
                "value": value
            })
            return True
        except Exception as e:
            self.log_entries.append({
                "action": "update_column_error",
                "item_id": item_id,
                "column_id": column_id,
                "error": str(e)[:100]
            })
            return False
    
    def update_item(self, item_id: str, item: Dict, mappings: List[Dict], target_board_id: str = TARGET_BOARD_ID):
        """Update existing item with new column values."""
        updates = []
        
        for mapping in mappings:
            source_col_id = mapping.get("source_column_id")
            target_col_id = mapping.get("target_column_id")
            merge_strategy = mapping.get("merge_strategy", "only_if_empty")
            transform = mapping.get("transform")
            
            if not target_col_id:
                continue
            
            # Get current target value
            target_item = self.duplicate_index["items"].get(item_id)
            target_col_value = None
            if target_item:
                target_col_value = self.get_column_value(target_item, target_col_id)
            
            # Check if we should update
            if not self.should_update_column(merge_strategy, target_col_value):
                continue
            
            # Special handling for calculate_salary transformation
            if transform == "calculate_salary":
                target_col_type = "numbers"  # Gehalt is a numbers column
                dummy_col_val = {"id": source_col_id, "text": "", "value": ""}
                column_value = self.prepare_column_value(
                    dummy_col_val, target_col_type, transform, item=item, mapping=mapping
                )
                if column_value:
                    updates.append({
                        "column_id": target_col_id,
                        "value": column_value
                    })
                continue
            
            # Special handling for gender_to_salutation transformation
            if transform == "gender_to_salutation":
                target_col_type = "dropdown"  # Anrede is a dropdown column
                dummy_col_val = {"id": source_col_id, "text": "", "value": ""}
                column_value = self.prepare_column_value(
                    dummy_col_val, target_col_type, transform, item=item, mapping=mapping
                )
                if column_value:
                    updates.append({
                        "column_id": target_col_id,
                        "value": column_value
                    })
                continue
            
            # Special handling for map_hours, map_languages, map_familienstand, map_nearest_city, and map_nationalitaet transformations
            if transform in ("map_hours", "map_languages", "map_familienstand", "map_nearest_city", "map_nationalitaet"):
                target_col_type = "dropdown"  # Both are dropdown columns
                dummy_col_val = {"id": source_col_id, "text": "", "value": ""}
                column_value = self.prepare_column_value(
                    dummy_col_val, target_col_type, transform, item=item, mapping=mapping
                )
                if column_value:
                    updates.append({
                        "column_id": target_col_id,
                        "value": column_value
                    })
                continue
            
            # Special handling for parse_number transformation
            if transform == "parse_number":
                target_col_type = "numbers"  # Kinder is a numbers column
                dummy_col_val = {"id": source_col_id, "text": "", "value": ""}
                column_value = self.prepare_column_value(
                    dummy_col_val, target_col_type, transform, item=item, mapping=mapping
                )
                if column_value:
                    updates.append({
                        "column_id": target_col_id,
                        "value": column_value
                    })
                continue
            
            # Special handling for map_country transformation
            if transform == "map_country":
                target_col_type = "dropdown"  # Geburtsland is a dropdown column
                dummy_col_val = {"id": source_col_id, "text": "", "value": ""}
                column_value = self.prepare_column_value(
                    dummy_col_val, target_col_type, transform, item=item, mapping=mapping
                )
                if column_value:
                    updates.append({
                        "column_id": target_col_id,
                        "value": column_value
                    })
                continue
            
            # Standard handling for other columns
            source_col_val = self.get_column_value(item, source_col_id)
            if not source_col_val:
                continue
            
            target_col_type = "text"  # Would need to fetch from board structure
            column_value = self.prepare_column_value(
                source_col_val, target_col_type, transform, item=item, mapping=mapping
            )
            
            if column_value:
                updates.append({
                    "column_id": target_col_id,
                    "value": column_value
                })
        
        if not updates:
            return
        
        # Batch update mutations
        for update in updates:
            mutation = """
            mutation ChangeColumnValue($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
                change_column_value(
                    board_id: $boardId,
                    item_id: $itemId,
                    column_id: $columnId,
                    value: $value,
                    create_labels_if_missing: true
                ) {
                    id
                }
            }
            """
            
            variables = {
                "boardId": target_board_id,
                "itemId": item_id,
                "columnId": update["column_id"],
                "value": update["value"]
            }
            
            try:
                self.client.execute_query(mutation, variables)
                time.sleep(0.2)  # Rate limit protection
            except Exception as e:
                self.log_entries.append({
                    "action": "update_error",
                    "item_id": item_id,
                    "board_id": target_board_id,
                    "column_id": update["column_id"],
                    "error": str(e)
                })
    
    def process_item(self, item: Dict, default_mappings: List[Dict], 
                    email_col_id: str, hf4u_col_id: str, 
                    candidate_id_col_id: Optional[str] = None):
        """Process a single item (create or update)."""
        item_name = item.get("name", "")
        
        # Check for duplicate
        duplicate_match = find_duplicate(
            item, self.duplicate_index, email_col_id, hf4u_col_id, candidate_id_col_id
        )

        # Name-only ambiguity: do NOT match, but log so we can investigate
        if duplicate_match and duplicate_match.get("match_type") == "name_only_ambiguous":
            self.log_entries.append({
                "action": "name_match_ambiguous",
                "item_name": item_name,
                "source_item_id": duplicate_match.get("source_item_id") or item.get("id"),
                "normalized_name": duplicate_match.get("normalized_name"),
                "candidates": duplicate_match.get("candidates", [])
            })
            duplicate_match = None
        
        if duplicate_match:
            target_item_id = duplicate_match["target_item_id"]
            source_item_id = duplicate_match["source_item_id"]
            match_type = duplicate_match.get("match_type", "unknown")
            
            # Get the board ID of the target item to use correct mapping
            target_board_id = self.get_item_board_id(target_item_id)
            if target_board_id:
                mapping_config = self.get_mapping_for_board(target_board_id)
                mappings = mapping_config.get("mappings", default_mappings)
            else:
                mappings = default_mappings
                target_board_id = TARGET_BOARD_ID
            
            # Move source item to duplicate group if configured
            if self.duplicate_group_id:
                if self.move_item_to_group(source_item_id, SOURCE_BOARD_ID, self.duplicate_group_id):
                    self.stats["moved_duplicates"] += 1
            
            # Link source item to the found duplicate via board-relation column
            self.link_source_to_duplicate(source_item_id, target_item_id)
            
            # Update existing item in target board
            self.update_item(target_item_id, item, mappings, target_board_id)
            
            # Transfer updates
            self.transfer_updates(item, target_item_id)
            
            self.stats["updated"] += 1
            self.log_entries.append({
                "action": "update",
                "item_name": item_name,
                "source_item_id": source_item_id,
                "target_item_id": target_item_id,
                "target_board_id": target_board_id,
                "match_type": match_type,
                "moved": bool(self.duplicate_group_id)
            })
        else:
            # Create new item (always in default TARGET_BOARD_ID)
            new_item_id = self.create_item(item, default_mappings)
            if new_item_id:
                # Move source item to "Neu" group if configured
                source_item_id = item.get("id")
                if self.new_group_id:
                    if self.move_item_to_group(source_item_id, SOURCE_BOARD_ID, self.new_group_id):
                        self.stats["moved_new"] += 1
                
                # Link source item to the newly created item via board-relation column
                self.link_source_to_duplicate(source_item_id, new_item_id)
                
                # Transfer updates
                self.transfer_updates(item, new_item_id)
                
                self.stats["created"] += 1
                self.log_entries.append({
                    "action": "create",
                    "item_name": item_name,
                    "item_id": new_item_id,
                    "source_item_id": source_item_id,
                    "moved_to_new": bool(self.new_group_id)
                })
            else:
                self.stats["errors"] += 1
    
    def merge_boards(self, email_col_id: str, hf4u_col_id: str, 
                    candidate_id_col_id: Optional[str] = None,
                    limit: Optional[int] = None, dry_run: bool = False):
        """Main merge process."""
        # Get default mappings (for TARGET_BOARD_ID, used for new items)
        default_config = self.get_mapping_for_board(TARGET_BOARD_ID)
        default_mappings = default_config.get("mappings", [])
        
        print(f"\nStarting merge process...")
        print(f"  Source board: {SOURCE_BOARD_ID}")
        print(f"  Target boards: {list(self.mapping_configs.keys())}")
        print(f"  Default mappings: {len(default_mappings)}")
        print(f"  Dry run: {dry_run}")
        
        if dry_run:
            print("\n[DRY RUN MODE - No changes will be made]")
        
        cursor = None
        page = 1
        processed = 0
        
        while True:
            print(f"\nProcessing page {page}...", end=" ", flush=True)
            result = self.client.get_all_items_paginated(SOURCE_BOARD_ID, cursor=cursor)
            items = result.get("items", [])
            
            if not items:
                break
            
            for item in items:
                if limit and processed >= limit:
                    break
                
                if not dry_run:
                    self.process_item(item, default_mappings, email_col_id, hf4u_col_id, candidate_id_col_id)
                else:
                    # Dry run: just check for duplicates
                    duplicate_match = find_duplicate(
                        item, self.duplicate_index, email_col_id, hf4u_col_id, candidate_id_col_id
                    )
                    if duplicate_match and duplicate_match.get("match_type") == "name_only_ambiguous":
                        # Don't treat ambiguous name-only as a duplicate; count as create
                        self.log_entries.append({
                            "action": "name_match_ambiguous",
                            "item_name": item.get("name", ""),
                            "source_item_id": duplicate_match.get("source_item_id") or item.get("id"),
                            "normalized_name": duplicate_match.get("normalized_name"),
                            "candidates": duplicate_match.get("candidates", [])
                        })
                        duplicate_match = None
                    if duplicate_match:
                        self.stats["updated"] += 1
                    else:
                        self.stats["created"] += 1
                
                processed += 1
                
                if processed % 100 == 0:
                    print(f"\n  Processed {processed} items...", end=" ", flush=True)
            
            if limit and processed >= limit:
                break
            
            cursor = result.get("cursor")
            if not cursor:
                break
            
            page += 1
            time.sleep(0.5)  # Rate limit protection
        
        # Print summary
        print(f"\n\n{'='*60}")
        print("Merge Summary:")
        print(f"  Items processed: {processed}")
        print(f"  Created: {self.stats['created']}")
        print(f"  Updated: {self.stats['updated']}")
        print(f"  Moved Duplicates (Source → 'Duplikate'): {self.stats['moved_duplicates']}")
        print(f"  Moved New Items (Source → 'Neu'): {self.stats['moved_new']}")
        print(f"  Errors: {self.stats['errors']}")
        print(f"{'='*60}")
        
        return self.stats, self.log_entries


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Merge Monday.com boards")
    parser.add_argument("--mapping", default="column_mapping.yaml", help="Column mapping YAML file")
    parser.add_argument("--index", default="output/duplicate_index.json", help="Duplicate index JSON file")
    parser.add_argument("--email-column", required=True, help="Email column ID")
    parser.add_argument("--hf4u-column", required=True, help="HF4U link column ID")
    parser.add_argument("--candidate-id-column", help="Candidate ID column ID (optional)")
    parser.add_argument("--limit", type=int, help="Limit number of items to process (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode (no changes)")
    parser.add_argument("--log", help="Log file path")
    
    args = parser.parse_args()
    
    # Load API token
    api_token = os.getenv("MONDAY_API_TOKEN")
    if not api_token:
        print("Error: MONDAY_API_TOKEN not found in .env file")
        sys.exit(1)
    
    # Load mapping configurations for all boards
    mapping_configs = {}
    print("Loading mapping configurations...")
    for board_id, mapping_file in BOARD_MAPPING_FILES.items():
        mapping_path = os.path.join(os.path.dirname(args.mapping), mapping_file)
        if os.path.exists(mapping_path):
            with open(mapping_path, 'r', encoding='utf-8') as f:
                mapping_configs[board_id] = yaml.safe_load(f)
                print(f"  Loaded {mapping_file} for board {board_id}")
        else:
            print(f"  Warning: {mapping_file} not found for board {board_id}")
    
    # Fallback: use args.mapping as default
    if not mapping_configs:
        with open(args.mapping, 'r', encoding='utf-8') as f:
            mapping_configs[TARGET_BOARD_ID] = yaml.safe_load(f)
            print(f"  Using fallback mapping: {args.mapping}")
    
    # Load duplicate index
    with open(args.index, 'r', encoding='utf-8') as f:
        duplicate_index = json.load(f)
    
    client = MondayAPIClient(api_token)
    
    # Find "Duplikate" group in source board
    print(f"\nChecking for 'Duplikate' group in source board {SOURCE_BOARD_ID}...")
    source_board_info = client.get_board_info(SOURCE_BOARD_ID)
    duplicate_group_id = None
    
    for group in source_board_info.get("groups", []):
        if group.get("title") == "Duplikate":
            duplicate_group_id = group.get("id")
            print(f"  Found 'Duplikate' group: {duplicate_group_id}")
            break
    
    if not duplicate_group_id:
        print("  Warning: 'Duplikate' group not found in source board. Duplicates will NOT be moved.")
    
    # Use hardcoded NEW_GROUP_ID for "Neu" group
    print(f"  Using 'Neu' group: {NEW_GROUP_ID}")
    
    merger = BoardMerger(client, mapping_configs, duplicate_index, duplicate_group_id, NEW_GROUP_ID)
    
    # Run merge
    stats, log_entries = merger.merge_boards(
        args.email_column,
        args.hf4u_column,
        args.candidate_id_column,
        args.limit,
        args.dry_run
    )
    
    # Save log
    if args.log:
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "stats": stats,
            "entries": log_entries
        }
        os.makedirs(os.path.dirname(args.log), exist_ok=True)
        with open(args.log, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)
        print(f"\nLog saved to: {args.log}")


if __name__ == "__main__":
    main()
