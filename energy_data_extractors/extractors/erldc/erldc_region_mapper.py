"""
ERLDC Region Mapper - Maps Eastern Regional power stations to their respective states and regional groups
"""
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

class ERLDCRegionMapper:
    def __init__(self):
        """Initialize ERLDC region mapper with station-to-state mappings"""
        
        # ERLDC Station to State mapping based on Eastern Regional constituents
        self.station_mappings = {
            # Bihar State Power Holding Company Limited
            'BSPHCL': 'Bihar',
            'BIHAR': 'Bihar',
            'BSEB': 'Bihar',
            
            # Damodar Valley Corporation (Multi-state: Jharkhand, West Bengal, Bihar, Odisha)
            'DVC': 'Multi-State',
            'DAMODAR': 'Multi-State',
            
            # West Bengal State Electricity Board
            'WBSEB': 'West Bengal',
            'WBSETCL': 'West Bengal',
            'WEST BENGAL': 'West Bengal',
            
            # Jharkhand State Electricity Board
            'JSEB': 'Jharkhand',
            'JBVNL': 'Jharkhand',
            'JHARKHAND': 'Jharkhand',
            
            # Odisha Power Generation Corporation
            'OPGC': 'Odisha',
            'OHPC': 'Odisha',
            'GRIDCO': 'Odisha',
            'ODISHA': 'Odisha',
            
            # Sikkim Power Development Corporation
            'SPDC': 'Sikkim',
            'SIKKIM': 'Sikkim',
            
            # NTPC Eastern Region stations
            'NTPC FARAKKA': 'West Bengal',
            'NTPC KAHALGAON': 'Bihar',
            'NTPC TALCHER': 'Odisha',
            'FARAKKA': 'West Bengal',
            'KAHALGAON': 'Bihar',
            'TALCHER': 'Odisha',
            
            # Central Sector stations in Eastern Region
            'MEJIA TPS': 'West Bengal',
            'KOLAGHAT TPS': 'West Bengal',
            'BAKRESWAR TPS': 'West Bengal',
            'BANDEL TPS': 'West Bengal',
            'DURGAPUR TPS': 'West Bengal',
            'SANTALDIH TPS': 'West Bengal',
            
            # Jharkhand stations
            'TENUGHAT': 'Jharkhand',
            'PATRATU': 'Jharkhand',
            'BOKARO': 'Jharkhand',
            
            # Bihar stations
            'MUZAFFARPUR': 'Bihar',
            'BARAUNI': 'Bihar',
            'KANTI': 'Bihar',
            
            # Odisha stations
            'TSTPS': 'Odisha',
            'KANIHA': 'Odisha',
            'IB': 'Odisha',
        }
        
        # Regional groups for Eastern Region
        self.regional_groups = {
            'Bihar': 'Eastern Plains',
            'West Bengal': 'Eastern Plains', 
            'Jharkhand': 'Eastern Plateau',
            'Odisha': 'Eastern Coastal',
            'Sikkim': 'Eastern Hills',
            'Multi-State': 'Multi-State'
        }
        
        logger.info(f"ERLDC Region Mapper initialized with {len(self.station_mappings)} station mappings")
    
    def get_state_from_station(self, station_name: str) -> str:
        """Map station name to its state"""
        if not station_name or pd.isna(station_name):
            return 'Unknown'
        
        # Clean and normalize station name
        station_clean = str(station_name).strip().upper()
        
        # Remove common suffixes and prefixes
        suffixes_to_remove = [' TPS', ' STPS', ' TPP', ' HEP', ' GPS', ' STP', ' PS', ' LTD', ' LIMITED']
        for suffix in suffixes_to_remove:
            if station_clean.endswith(suffix):
                station_clean = station_clean[:-len(suffix)].strip()
        
        # Direct mapping
        if station_clean in self.station_mappings:
            return self.station_mappings[station_clean]
        
        # Partial matching for complex names
        for mapped_station, state in self.station_mappings.items():
            if mapped_station in station_clean or station_clean in mapped_station:
                return state
        
        # If no match found, try to infer from common patterns
        if any(keyword in station_clean for keyword in ['BIHAR', 'BSPHCL', 'BSEB']):
            return 'Bihar'
        elif any(keyword in station_clean for keyword in ['BENGAL', 'WB', 'WBSEB']):
            return 'West Bengal'
        elif any(keyword in station_clean for keyword in ['JHARKHAND', 'JSEB', 'JBVNL']):
            return 'Jharkhand'
        elif any(keyword in station_clean for keyword in ['ODISHA', 'ORISSA', 'OPGC', 'OHPC']):
            return 'Odisha'
        elif any(keyword in station_clean for keyword in ['SIKKIM', 'SPDC']):
            return 'Sikkim'
        elif any(keyword in station_clean for keyword in ['DVC', 'DAMODAR']):
            return 'Multi-State'
        
        return 'Unknown'
    
    def get_regional_group(self, state: str) -> str:
        """Get regional group for a state"""
        return self.regional_groups.get(state, 'Unknown')
    
    def map_station_to_region(self, station_name: str) -> Tuple[str, str]:
        """Map station to both state and regional group"""
        state = self.get_state_from_station(station_name)
        group = self.get_regional_group(state)
        return state, group
    
    def map_dataframe_regions(self, df: pd.DataFrame, station_column: str) -> pd.DataFrame:
        """Add state and regional group columns to dataframe"""
        df_copy = df.copy()
        
        # Apply mapping
        df_copy[['State', 'Regional_Group']] = df_copy[station_column].apply(
            lambda x: pd.Series(self.map_station_to_region(x))
        )
        
        return df_copy
    
    def get_region_summary(self, df: pd.DataFrame, station_column: str) -> Dict:
        """Get summary of regions in the dataframe"""
        if 'State' not in df.columns or 'Regional_Group' not in df.columns:
            df = self.map_dataframe_regions(df, station_column)
        
        summary = {
            'total_records': len(df),
            'unique_stations': df[station_column].nunique(),
            'by_state': df['State'].value_counts().to_dict(),
            'by_group': df['Regional_Group'].value_counts().to_dict(),
            'states_covered': df['State'].nunique(),
            'groups_covered': df['Regional_Group'].nunique()
        }
        
        return summary
    
    def get_available_regions(self) -> Dict:
        """Get all available regions and mappings"""
        return {
            'states': list(set(self.station_mappings.values())),
            'groups': list(set(self.regional_groups.values())),
            'total_mapped_stations': len(self.station_mappings)
        }

def test_erldc_mapper():
    """Test the ERLDC region mapper"""
    mapper = ERLDCRegionMapper()
    
    # Test stations
    test_stations = ['BSPHCL', 'DVC', 'NTPC FARAKKA', 'WBSEB', 'OPGC', 'Unknown Station']
    
    print("ERLDC Region Mapping Test:")
    print("=" * 50)
    
    for station in test_stations:
        state, group = mapper.map_station_to_region(station)
        print(f"{station:<15} -> {state:<15} ({group})")
    
    print(f"\nTotal mapped stations: {len(mapper.station_mappings)}")
    print(f"Available regions: {mapper.get_available_regions()}")

if __name__ == "__main__":
    test_erldc_mapper()
