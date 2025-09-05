"""
WRPC (Western Regional Power Committee) Region Mapper
Maps Western Regional power stations to their respective states and regional groups
"""

import logging
import json
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

class WRPCRegionMapper:
    """Maps WRPC power stations to their geographical regions and states"""
    
    def __init__(self):
        self.station_mapping = self._initialize_station_mapping()
        self.state_groups = self._initialize_state_groups()
    
    def _initialize_station_mapping(self) -> Dict[str, Dict[str, str]]:
        """Initialize mapping of WRPC stations to states and regional groups"""
        return {
            # Gujarat Stations
            'GSECL': {'state': 'Gujarat', 'group': 'Western Coastal'},
            'GUVNL': {'state': 'Gujarat', 'group': 'Western Coastal'},
            'GPEC': {'state': 'Gujarat', 'group': 'Western Coastal'},
            'GTPS': {'state': 'Gujarat', 'group': 'Western Coastal'},
            'KLTPS': {'state': 'Gujarat', 'group': 'Western Coastal'},
            'WCTPS': {'state': 'Gujarat', 'group': 'Western Coastal'},
            'UNOSUGEN': {'state': 'Gujarat', 'group': 'Western Coastal'},
            'KAWAS': {'state': 'Gujarat', 'group': 'Western Coastal'},
            'GANDHAR': {'state': 'Gujarat', 'group': 'Western Coastal'},
            'SABARMATI': {'state': 'Gujarat', 'group': 'Western Coastal'},
            'ACBIL': {'state': 'Gujarat', 'group': 'Western Coastal'},
            
            # Maharashtra Stations
            'MAHAGENCO': {'state': 'Maharashtra', 'group': 'Western Plateau'},
            'MSEDCL': {'state': 'Maharashtra', 'group': 'Western Plateau'},
            'TATA POWER': {'state': 'Maharashtra', 'group': 'Western Plateau'},
            'RELIANCE': {'state': 'Maharashtra', 'group': 'Western Plateau'},
            'ADANI': {'state': 'Maharashtra', 'group': 'Western Plateau'},
            'KORADI': {'state': 'Maharashtra', 'group': 'Western Plateau'},
            'CHANDRAPUR': {'state': 'Maharashtra', 'group': 'Western Plateau'},
            'NASHIK': {'state': 'Maharashtra', 'group': 'Western Plateau'},
            'BHIRA': {'state': 'Maharashtra', 'group': 'Western Plateau'},
            'TARAPUR': {'state': 'Maharashtra', 'group': 'Western Coastal'},
            'MSPGCL': {'state': 'Maharashtra', 'group': 'Western Plateau'},
            
            # Madhya Pradesh Stations
            'MPPGCL': {'state': 'Madhya Pradesh', 'group': 'Central Plateau'},
            'MPPMCL': {'state': 'Madhya Pradesh', 'group': 'Central Plateau'},
            'SASAN': {'state': 'Madhya Pradesh', 'group': 'Central Plateau'},
            'VINDHYACHAL': {'state': 'Madhya Pradesh', 'group': 'Central Plateau'},
            'SATPURA': {'state': 'Madhya Pradesh', 'group': 'Central Plateau'},
            'AMARKANTAK': {'state': 'Madhya Pradesh', 'group': 'Central Plateau'},
            'SHREE SINGAJI': {'state': 'Madhya Pradesh', 'group': 'Central Plateau'},
            'MPPTCL': {'state': 'Madhya Pradesh', 'group': 'Central Plateau'},
            
            # Chhattisgarh Stations
            'CSPDCL': {'state': 'Chhattisgarh', 'group': 'Central Plateau'},
            'NTPC SIPAT': {'state': 'Chhattisgarh', 'group': 'Central Plateau'},
            'KORBA': {'state': 'Chhattisgarh', 'group': 'Central Plateau'},
            'BHILAI': {'state': 'Chhattisgarh', 'group': 'Central Plateau'},
            'CSPTCL': {'state': 'Chhattisgarh', 'group': 'Central Plateau'},
            
            # Rajasthan Stations
            'RVUNL': {'state': 'Rajasthan', 'group': 'Western Desert'},
            'RRVUNL': {'state': 'Rajasthan', 'group': 'Western Desert'},
            'SURATGARH': {'state': 'Rajasthan', 'group': 'Western Desert'},
            'CHHABRA': {'state': 'Rajasthan', 'group': 'Western Desert'},
            'KALISINDH': {'state': 'Rajasthan', 'group': 'Western Desert'},
            'BANSWARA': {'state': 'Rajasthan', 'group': 'Western Desert'},
            'RSTPS': {'state': 'Rajasthan', 'group': 'Western Desert'},
            
            # Goa Stations
            'GEDA': {'state': 'Goa', 'group': 'Western Coastal'},
            
            # Daman & Diu Stations
            'DNH POWER': {'state': 'Daman & Diu', 'group': 'Western Coastal'},
            
            # Multi-State Entities
            'NTPC': {'state': 'Multi-State', 'group': 'Multi-State'},
            'NHPC': {'state': 'Multi-State', 'group': 'Multi-State'},
            'POWERGRID': {'state': 'Multi-State', 'group': 'Multi-State'},
            'PGCIL': {'state': 'Multi-State', 'group': 'Multi-State'},
        }
    
    def _initialize_state_groups(self) -> Dict[str, List[str]]:
        """Initialize regional groups and their constituent states"""
        return {
            'Western Coastal': ['Gujarat', 'Maharashtra', 'Goa', 'Daman & Diu'],
            'Western Plateau': ['Maharashtra'],
            'Central Plateau': ['Madhya Pradesh', 'Chhattisgarh'],
            'Western Desert': ['Rajasthan'],
            'Multi-State': ['Multi-State']
        }
    
    def normalize_station_name(self, station_name: str) -> str:
        """Normalize station name for matching"""
        if not station_name:
            return ""
        return station_name.upper().strip()
    
    def map_station_to_region(self, station_name: str) -> Tuple[str, str]:
        """Map a station name to its state and regional group"""
        try:
            normalized_name = self.normalize_station_name(station_name)
            
            # Direct mapping
            if normalized_name in self.station_mapping:
                mapping = self.station_mapping[normalized_name]
                return mapping['state'], mapping['group']
            
            # Partial matching for complex station names
            for mapped_station, mapping in self.station_mapping.items():
                if mapped_station in normalized_name or normalized_name in mapped_station:
                    logger.info(f"🔍 Partial match found: {station_name} -> {mapped_station}")
                    return mapping['state'], mapping['group']
            
            # Keyword-based fallback matching
            state_keywords = {
                'Gujarat': ['GUJ', 'GUJARAT', 'GSECL', 'GUVNL', 'KAWAS', 'GANDHAR'],
                'Maharashtra': ['MAH', 'MAHARASHTRA', 'MSEDCL', 'TATA', 'KORADI', 'CHANDRAPUR'],
                'Madhya Pradesh': ['MP', 'MADHYA', 'PRADESH', 'MPPGCL', 'VINDHYACHAL', 'SATPURA'],
                'Chhattisgarh': ['CG', 'CHHATTISGARH', 'CSPDCL', 'SIPAT', 'KORBA'],
                'Rajasthan': ['RAJ', 'RAJASTHAN', 'RVUNL', 'SURATGARH', 'CHHABRA'],
                'Goa': ['GOA', 'GEDA'],
                'Multi-State': ['NTPC', 'NHPC', 'POWERGRID', 'PGCIL']
            }
            
            for state, keywords in state_keywords.items():
                if any(keyword in normalized_name for keyword in keywords):
                    group = self._get_group_for_state(state)
                    logger.info(f"🔍 Keyword match found: {station_name} -> {state} ({group})")
                    return state, group
            
            # Unknown station
            logger.warning(f"⚠️ Unknown WRPC station: {station_name}")
            return 'Unknown', 'Unknown'
            
        except Exception as e:
            logger.error(f"❌ Error mapping station {station_name}: {e}")
            return 'Unknown', 'Unknown'
    
    def _get_group_for_state(self, state: str) -> str:
        """Get regional group for a given state"""
        for group, states in self.state_groups.items():
            if state in states:
                return group
        return 'Unknown'
    
    def get_all_stations(self) -> List[str]:
        """Get list of all mapped stations"""
        return list(self.station_mapping.keys())
    
    def get_states_in_group(self, group: str) -> List[str]:
        """Get all states in a regional group"""
        return self.state_groups.get(group, [])
    
    def get_stations_by_state(self, state: str) -> List[str]:
        """Get all stations in a specific state"""
        return [station for station, mapping in self.station_mapping.items() 
                if mapping['state'] == state]
    
    def get_stations_by_group(self, group: str) -> List[str]:
        """Get all stations in a specific regional group"""
        return [station for station, mapping in self.station_mapping.items() 
                if mapping['group'] == group]
    
    def generate_summary(self, station_counts: Dict[str, int]) -> Dict:
        """Generate a summary of regional distribution"""
        try:
            state_distribution = {}
            group_distribution = {}
            total_stations = 0
            
            for station, count in station_counts.items():
                state, group = self.map_station_to_region(station)
                
                # Update state distribution
                if state in state_distribution:
                    state_distribution[state] += count
                else:
                    state_distribution[state] = count
                
                # Update group distribution
                if group in group_distribution:
                    group_distribution[group] += count
                else:
                    group_distribution[group] = count
                
                total_stations += count
            
            return {
                'total_stations': len(station_counts),
                'states_covered': len(state_distribution),
                'regional_groups': len(group_distribution),
                'state_distribution': state_distribution,
                'group_distribution': group_distribution,
                'total_records': total_stations
            }
            
        except Exception as e:
            logger.error(f"❌ Error generating WRPC summary: {e}")
            return {}

def test_wrpc_mapper():
    """Test the WRPC region mapper"""
    mapper = WRPCRegionMapper()
    
    # Test stations
    test_stations = ['ACBIL', 'GSECL', 'MAHAGENCO', 'MPPGCL', 'RVUNL', 'NTPC', 'UNKNOWN_STATION']
    
    print("🧪 Testing WRPC Region Mapper:")
    print("=" * 50)
    
    for station in test_stations:
        state, group = mapper.map_station_to_region(station)
        print(f"📍 {station:15} -> {state:15} | {group}")
    
    print("\n📊 Regional Groups:")
    print("=" * 30)
    for group, states in mapper.state_groups.items():
        print(f"🏔️  {group:20} -> {', '.join(states)}")
    
    print(f"\n📈 Total Mapped Stations: {len(mapper.get_all_stations())}")

if __name__ == "__main__":
    test_wrpc_mapper()
