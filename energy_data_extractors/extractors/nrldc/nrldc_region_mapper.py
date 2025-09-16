#!/usr/bin/env python3
"""
NRLDC Region Mapper - Maps NRLDC power stations to their respective states/regions
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class NRLDCRegionMapper:
    def __init__(self):
        """Initialize NRLDC region mapping"""
        self.region_mapping = {
            # Himachal Pradesh Stations
            'BHAKRA': 'Himachal Pradesh',
            'CHAMERA': 'Himachal Pradesh',
            'CHAMERA-II': 'Himachal Pradesh',
            'CHAMERA-III': 'Himachal Pradesh',
            'DEHAR': 'Himachal Pradesh',
            'KARCHAM': 'Himachal Pradesh',
            'KOLDAM': 'Himachal Pradesh',
            'NATHPA': 'Himachal Pradesh',
            'PARBATI-II': 'Himachal Pradesh',
            'PARBATI-III': 'Himachal Pradesh',
            'PONG': 'Himachal Pradesh',
            'RAMPUR': 'Himachal Pradesh',
            'SAINJ': 'Himachal Pradesh',
            'SEWA-II': 'Himachal Pradesh',
            'SINGOLI': 'Himachal Pradesh',
            
            # Jammu & Kashmir Stations
            'DULHASTI': 'Jammu & Kashmir',
            'KISHANGANGA': 'Jammu & Kashmir',
            'SALAL': 'Jammu & Kashmir',
            'URI': 'Jammu & Kashmir',
            'URI-II': 'Jammu & Kashmir',
            
            # Uttarakhand Stations
            'DHAULIGANGA': 'Uttarakhand',
            'KOTESHWAR': 'Uttarakhand',
            'TEHRI': 'Uttarakhand',
            'TANAKPUR': 'Uttarakhand',
            'SORANG': 'Uttarakhand',
            
            # Uttar Pradesh Stations
            'AURAIYA': 'Uttar Pradesh',
            'DADRI': 'Uttar Pradesh',
            'DADRI-II': 'Uttar Pradesh',
            'RIHAND': 'Uttar Pradesh',
            'RIHAND-II': 'Uttar Pradesh',
            'RIHAND-III': 'Uttar Pradesh',
            'UNCHAHAR-I': 'Uttar Pradesh',
            'UNCHAHAR-II': 'Uttar Pradesh',
            'UNCHAHAR-III': 'Uttar Pradesh',
            'UNCHAHAR-IV': 'Uttar Pradesh',
            'TANDA-II': 'Uttar Pradesh',
            
            # Madhya Pradesh Stations
            'BAIRASIUL': 'Madhya Pradesh',
            'SINGRAULI': 'Madhya Pradesh',
            'SCL': 'Madhya Pradesh',
            
            # Haryana Stations
            'IGSTPS-JHAJJAR': 'Haryana',
            
            # Rajasthan Stations
            'RAPPB': 'Rajasthan',
            'RAPPC': 'Rajasthan',
            'RAPPD': 'Rajasthan',
            'ADHPL': 'Rajasthan',
            
            # Multi-state/Central Stations
            'NAPP': 'Multi-State',  # Narora Atomic Power Plant
            'BUDHIL': 'Himachal Pradesh',
            'ANTA': 'Rajasthan',  # Anta Gas Power Station
            
            # Additional Eastern Region Stations (mapped to their states)
            'BARH-I': 'Bihar',
            'FARAKKA': 'West Bengal',
            'FARAKKA-III': 'West Bengal',
            'KAHALGAON-II': 'Bihar',
            'KAHALGAON1': 'Bihar',
            'NABINAGAR': 'Bihar',
            
            # Additional Western Region Stations
            'GANDHAR': 'Gujarat',
            'KAWAS': 'Gujarat',
            'KAKRAPAR': 'Gujarat',
            'MUNDRA_UMPP': 'Gujarat',
            
            # Additional Southern Region Stations
            'KAIGA': 'Karnataka',
            'KUDGI': 'Karnataka',
            'KUNDANKULAM': 'Tamil Nadu',
            'MADRAS': 'Tamil Nadu',
            'NEYVELI': 'Tamil Nadu',
            'NLC': 'Tamil Nadu',
            'SIMHADRI': 'Andhra Pradesh',
            'SIMHADRI-II': 'Andhra Pradesh',
            'RAMAGUNDAM': 'Telangana',
            'RAAMAGUNDAM': 'Telangana',
            'NTPL': 'Telangana',
            
            # Additional Central Region Stations
            'KORBA': 'Chhattisgarh',
            'SIPAT': 'Chhattisgarh',
            'SASAN': 'Madhya Pradesh',
            'GADARWARA': 'Madhya Pradesh',
            'KHARGONE-I': 'Madhya Pradesh',
            'MOUDA': 'Maharashtra',
            'SOLAPUR': 'Maharashtra',
            
            # Additional Eastern Region Stations
            'DARLIPALI': 'Odisha',
            'LARA-I': 'Odisha',
            'BONGAIGAON': 'Assam',
            'KAMENG': 'Arunachal Pradesh',
            'MANGDECHU': 'Arunachal Pradesh',
            
            # Final missing stations for 100% coverage
            'TALA': 'Bhutan',  # Tala HEP - Bhutan (cross-border)
            'TALCHER': 'Odisha',  # Talcher Stage 2
            'TARAPUR': 'Maharashtra',  # Tarapur Atomic Power
            'TELANGANASTPP': 'Telangana',  # Telangana STPP
            'VIDHYACHAL': 'Madhya Pradesh',  # Vidhyachal STPS variants
            'VINDHYACHAL': 'Madhya Pradesh',  # Vindhyachal STPS variants
            'VTPS': 'Madhya Pradesh',  # Vindhyachal TPS
        }
        
        # State-wise groupings for NRLDC region
        self.state_groups = {
            'Northern Hills': ['Himachal Pradesh', 'Jammu & Kashmir', 'Uttarakhand'],
            'Northern Plains': ['Uttar Pradesh', 'Haryana'],
            'Western': ['Rajasthan', 'Gujarat'],
            'Central': ['Madhya Pradesh', 'Chhattisgarh', 'Maharashtra'],
            'Eastern': ['West Bengal', 'Bihar', 'Odisha', 'Assam'],
            'Southern': ['Karnataka', 'Tamil Nadu', 'Andhra Pradesh', 'Telangana'],
            'North Eastern': ['Arunachal Pradesh'],
            'Cross-Border': ['Bhutan'],
            'Multi-State': ['Multi-State']
        }
        
    def get_station_region(self, station_name):
        """Get region for a specific station"""
        # Clean and normalize station name
        station_upper = station_name.upper().strip()
        
        # Handle special cases with suffixes and variations
        base_station = station_upper
        
        # Remove common suffixes
        suffixes_to_remove = [' GF', ' LF', ' RF', ' AF', ' CRF', ' STPS', ' STPP', ' TPP', ' TPS', ' HEP', ' GS', ' NPP']
        for suffix in suffixes_to_remove:
            if base_station.endswith(suffix):
                base_station = base_station[:-len(suffix)]
                break
        
        # Handle specific station name variations
        if 'CHAMERA' in base_station:
            if 'II' in station_name:
                base_station = 'CHAMERA-II'
            elif 'III' in station_name:
                base_station = 'CHAMERA-III'
            else:
                base_station = 'CHAMERA'
        elif 'FARAKKA-III' in station_upper:
            base_station = 'FARAKKA-III'
        elif 'FARAKKA' in base_station:
            base_station = 'FARAKKA'
        elif 'KAHALGAON' in base_station:
            if '1' in base_station or 'I' in base_station:
                base_station = 'KAHALGAON1'
            elif 'II' in base_station:
                base_station = 'KAHALGAON-II'
        elif 'GANDHAR' in base_station:
            base_station = 'GANDHAR'
        elif 'KAWAS' in base_station:
            base_station = 'KAWAS'
        elif 'KAKRAPAR' in base_station:
            base_station = 'KAKRAPAR'
        elif 'KAIGA' in base_station:
            base_station = 'KAIGA'
        elif 'KUNDANKULAM' in base_station:
            base_station = 'KUNDANKULAM'
        elif 'MADRAS' in base_station and 'ATOMIC' in station_upper:
            base_station = 'MADRAS'
        elif 'NEYVELI' in base_station or 'NLC' in base_station:
            base_station = 'NLC'
        elif 'SIMHADRI' in base_station:
            if 'II' in base_station:
                base_station = 'SIMHADRI-II'
            else:
                base_station = 'SIMHADRI'
        elif 'RAMAGUNDAM' in base_station or 'RAAMAGUNDAM' in base_station:
            base_station = 'RAMAGUNDAM'
        elif 'KORBA' in base_station:
            base_station = 'KORBA'
        elif 'SIPAT' in base_station:
            base_station = 'SIPAT'
        elif 'SASAN' in base_station:
            base_station = 'SASAN'
        elif 'GADARWARA' in base_station:
            base_station = 'GADARWARA'
        elif 'KHARGONE' in base_station:
            base_station = 'KHARGONE-I'
        elif 'MOUDA' in base_station:
            base_station = 'MOUDA'
        elif 'SOLAPUR' in base_station:
            base_station = 'SOLAPUR'
        elif 'DARLIPALI' in base_station:
            base_station = 'DARLIPALI'
        elif 'LARA' in base_station:
            base_station = 'LARA-I'
        elif 'BONGAIGAON' in base_station:
            base_station = 'BONGAIGAON'
        elif 'KAMENG' in base_station:
            base_station = 'KAMENG'
        elif 'MANGDECHU' in base_station:
            base_station = 'MANGDECHU'
        elif 'BARH' in base_station:
            base_station = 'BARH-I'
        elif 'NABINAGAR' in base_station:
            base_station = 'NABINAGAR'
        elif 'MUNDRA' in base_station:
            base_station = 'MUNDRA_UMPP'
        elif 'KUDGI' in base_station:
            base_station = 'KUDGI'
        elif 'VTPS' in base_station:
            base_station = 'VTPS'
        elif 'TALA' in base_station:
            base_station = 'TALA'
        elif 'TALCHER' in base_station:
            base_station = 'TALCHER'
        elif 'TARAPUR' in base_station:
            base_station = 'TARAPUR'
        elif 'TELANGANA' in base_station:
            base_station = 'TELANGANASTPP'
        elif 'VIDHYACHAL' in base_station:
            base_station = 'VIDHYACHAL'
        elif 'VINDHYACHAL' in base_station:
            base_station = 'VINDHYACHAL'
        
        # First try exact match
        if base_station in self.region_mapping:
            return self.region_mapping[base_station]
        
        # Try partial matching for remaining cases
        for mapped_station, region in self.region_mapping.items():
            if mapped_station in base_station or base_station in mapped_station:
                return region
        
        return 'Unknown'
    
    def get_station_group(self, station_name):
        """Get regional group for a station"""
        state = self.get_station_region(station_name)
        
        for group, states in self.state_groups.items():
            if state in states:
                return group
        
        return 'Unknown'
    
    def map_dataframe_regions(self, df, station_column='Stn_Name'):
        """Add region and group columns to dataframe"""
        if station_column not in df.columns:
            logger.warning(f"Column {station_column} not found in dataframe")
            return df
        
        # Create copies to avoid modifying original
        df_mapped = df.copy()
        
        # Add region mapping
        df_mapped['State'] = df_mapped[station_column].apply(self.get_station_region)
        df_mapped['Regional_Group'] = df_mapped[station_column].apply(self.get_station_group)
        
        return df_mapped
    
    def get_region_summary(self, df, station_column='Stn_Name'):
        """Get summary of stations by region"""
        df_mapped = self.map_dataframe_regions(df, station_column)
        
        region_summary = {
            'by_state': df_mapped['State'].value_counts().to_dict(),
            'by_group': df_mapped['Regional_Group'].value_counts().to_dict(),
            'total_stations': len(df_mapped[station_column].unique())
        }
        
        return region_summary
    
    def get_all_regions(self):
        """Get all available regions and groups"""
        return {
            'states': list(set(self.region_mapping.values())),
            'groups': list(self.state_groups.keys()),
            'total_mapped_stations': len(self.region_mapping)
        }

def main():
    """Test the region mapper"""
    mapper = NRLDCRegionMapper()
    
    # Test with some sample stations
    test_stations = ['ADHPL', 'ANTA GF', 'BHAKRA', 'CHAMERA-II', 'DADRI', 'URI']
    
    print("NRLDC Region Mapping Test:")
    print("=" * 50)
    
    for station in test_stations:
        region = mapper.get_station_region(station)
        group = mapper.get_station_group(station)
        print(f"{station:15} -> {region:20} ({group})")
    
    print(f"\nTotal mapped stations: {len(mapper.region_mapping)}")
    print(f"Available regions: {mapper.get_all_regions()}")

if __name__ == "__main__":
    main()
