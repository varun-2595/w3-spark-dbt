import os
import csv

OUTPUT_DIR = "data"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "taxi_zone_lookup.csv")

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR", "Unknown"]
SERVICE_ZONES = ["Yellow Zone", "Boro Zone", "Airports", "N/A"]

def generate_mock_lookup():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    print(f"Generating mock taxi zone lookup offline at {OUTPUT_PATH}...")
    
    with open(OUTPUT_PATH, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        
        # 1 to 263 are valid LocationIDs
        for loc_id in range(1, 264):
            if loc_id == 264 or loc_id == 265:
                # 264 and 265 are sometimes NV or Unknown, let's keep it simple
                continue
            
            # Create somewhat realistic assignments
            if loc_id % 7 == 0:
                borough = "Unknown"
                zone = "Unknown"
                service_zone = "N/A"
            elif loc_id % 6 == 0:
                borough = "EWR"
                zone = "Newark Airport"
                service_zone = "Airports"
            elif loc_id % 5 == 0:
                borough = "Staten Island"
                zone = f"SI Zone {loc_id}"
                service_zone = "Boro Zone"
            elif loc_id % 4 == 0:
                borough = "Bronx"
                zone = f"BX Zone {loc_id}"
                service_zone = "Boro Zone"
            elif loc_id % 3 == 0:
                borough = "Queens"
                zone = f"QNS Zone {loc_id}"
                service_zone = "Boro Zone" if loc_id % 2 == 0 else "Airports"
            elif loc_id % 2 == 0:
                borough = "Brooklyn"
                zone = f"BK Zone {loc_id}"
                service_zone = "Boro Zone"
            else:
                borough = "Manhattan"
                zone = f"MHT Zone {loc_id}"
                service_zone = "Yellow Zone"
                
            writer.writerow([loc_id, borough, zone, service_zone])
            
    print(f"Successfully generated {OUTPUT_PATH}")

if __name__ == "__main__":
    generate_mock_lookup()
